import asyncio
import components.system
import fileinput
import json
import os

from config import defaults
from components.models.system import SystemSettings, UpdateSystemSettings
from components.utils import batch, expire_key
from components.utils.datetimes import datetime, ntime_utc_now
from components.web.utils import *

blueprint = Blueprint("system", __name__, url_prefix="/system")

LOG_LOCK = asyncio.Lock()
APP_LOGS_FULL_PULL = dict()
APP_LOGS_LAST_REFRESH = None


@blueprint.before_request
async def before_request():
    global L
    L = LANG[request.USER_LANG]


@blueprint.context_processor
def load_context():
    return {
        "schemas": {"system_settings": SystemSettings.model_json_schema()},
        "L": L,
    }
    return context


@blueprint.route("/status/refresh", methods=["POST"])
@acl("system")
async def status_refresh():
    async with cluster.receiving:
        sent = await cluster.send_command("STATUS", "*")
        await cluster.await_receivers(sent, raise_err=False)

    return await status()


@blueprint.route("/status", methods=["GET"])
@acl("system")
async def status():
    status = {
        "CLUSTER_PEERS_REMOTE_PEERS": cluster.peers.remotes,
        "CLUSTER_PEERS_LOCAL": cluster.peers.local,
    }
    return await render_template("system/status.html", data={"status": status})


@blueprint.route("/settings", methods=["PATCH"])
@acl("system")
async def write_settings():
    try:
        UpdateSystemSettingsModel = UpdateSystemSettings.parse_obj(request.form_parsed)
    except ValidationError as e:
        return validation_error(e.errors())

    async with ClusterLock("system_settings"):
        async with TinyDB(**dbparams()) as db:
            db.table("system_settings").upsert(
                Document(UpdateSystemSettingsModel.dict(), doc_id=1)
            )

    return trigger_notification(
        level="success",
        response_code=204,
        title="Settings updated",
        message="System settings were updated",
    )


@blueprint.route("/settings")
@acl("system")
async def settings():
    try:
        settings = await components.system.get_system_settings()
    except ValidationError as e:
        return validation_error(e.errors())

    return await render_template("system/settings.html", settings=settings.dict())


@blueprint.route("/logs")
@blueprint.route("/logs/search", methods=["POST"])
@acl("system")
async def cluster_logs():
    try:
        (
            q,
            page,
            page_size,
            sort_attr,
            sort_reverse,
            filters,
        ) = table_search_helper(
            request.form_parsed,
            "system_logs",
            default_sort_attr="record.time.repr",
            default_sort_reverse=True,
        )
    except ValidationError as e:
        return validation_error(e.errors())

    if request.method == "POST":
        _logs = []
        async with LOG_LOCK:
            parser_failed = False

            with fileinput.input(
                components.system.list_application_log_files(), encoding="utf-8"
            ) as f:
                for line in f:
                    if q in line:
                        try:
                            _logs.append(json.loads(line.strip()))
                        except json.decoder.JSONDecodeError:
                            parser_failed = True
                            os.unlink(f.filename())
                            f.nextfile()

            if parser_failed:
                return trigger_notification(
                    level="user",
                    response_code=204,
                    title="Full refresh",
                    message="Logs rotated, requesting full refresh...",
                    additional_triggers={"forceRefresh": ""},
                    duration=1000,
                )

        def system_logs_sort_func(sort_attr):
            if sort_attr == "text":
                return lambda d: (
                    d["text"],
                    datetime.fromisoformat(d["record"]["time"]["repr"]).timestamp(),
                )
            elif sort_attr == "record.level.no":
                return lambda d: (
                    d["record"]["level"]["no"],
                    datetime.fromisoformat(d["record"]["time"]["repr"]).timestamp(),
                )
            else:  # fallback to "record.time.repr"
                return lambda d: datetime.fromisoformat(
                    d["record"]["time"]["repr"]
                ).timestamp()

        log_pages = [
            m
            for m in batch(
                sorted(
                    _logs,
                    key=system_logs_sort_func(sort_attr),
                    reverse=sort_reverse,
                ),
                page_size,
            )
        ]

        try:
            log_pages[page - 1]
        except IndexError:
            page = len(log_pages)

        system_logs = log_pages[page - 1] if page else log_pages

        return await render_template(
            "system/includes/logs/table_body.html",
            data={
                "logs": system_logs,
                "page_size": page_size,
                "page": page,
                "pages": len(log_pages),
                "elements": len(_logs),
            },
        )
    else:
        return await render_template("system/logs.html")


@blueprint.route("/logs/refresh")
@acl("system")
async def refresh_cluster_logs():
    await ws_htmx(
        session["login"],
        "beforeend",
        '<div class="loading-logs" hidden _="on load trigger '
        + "notification("
        + "title: 'Please wait', level: 'user', "
        + "message: 'Requesting logs, your view will be updated automatically.', duration: 10000)\">"
        + "</div>",
        "/system/logs",
    )

    if (
        not APP_LOGS_LAST_REFRESH
        or request.args.get("force")
        or (
            round(ntime_utc_now() - APP_LOGS_LAST_REFRESH)
            >= defaults.CLUSTER_LOGS_REFRESH_AFTER
        )
    ):
        APP_LOGS_LAST_REFRESH = ntime_utc_now()

        async with LOG_LOCK:
            async with ClusterLock("files"):
                for peer in cluster.peers.get_established():
                    if not peer in APP_LOGS_FULL_PULL:
                        APP_LOGS_FULL_PULL[peer] = True
                        current_app.add_background_task(
                            expire_key,
                            APP_LOGS_FULL_PULL,
                            peer,
                            36000,
                        )
                        startb = 0
                    else:
                        startb = -1
                        file_path = f"peer_files/{peer}/logs/application.log"
                        if os.path.exists(file_path) and os.path.getsize(file_path) > (
                            5 * 1024 * 1024
                        ):
                            startb = 0

                    await cluster.files.fileget(
                        "logs/application.log",
                        f"peer_files/{peer}/logs/application.log",
                        peer,
                        startb,
                        -1,
                    )

            missing_peers = ", ".join(cluster.peers.get_offline_peers())

            if missing_peers:
                await ws_htmx(
                    session["login"],
                    "beforeend",
                    '<div hidden _="on load trigger '
                    + "notification("
                    + "title: 'Missing peers', level: 'warning', "
                    + f"message: 'Some peers seem to be offline and were not pulled: {missing_peers}', duration: 3000)\">"
                    + "</div>",
                    "/system/logs",
                )

    refresh_ago = round(ntime_utc_now() - APP_LOGS_LAST_REFRESH)

    await ws_htmx(
        session["login"],
        "beforeend",
        '<div hidden _="on load trigger logsReady on #system-logs-table-search '
        + f"then put {refresh_ago} into #system-logs-last-refresh "
        + f'then wait 500 ms then trigger removeNotification on .notification-user"></div>',
        "/system/logs",
    )

    return "", 204
