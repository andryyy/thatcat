import asyncio
import fileinput
import json
import os

from components.cluster import cluster
from components.database import db
from components.models.system import SystemSettings, SystemSettingsPatch
from components.utils.datetimes import datetime
from components.utils.misc import batch
from components.utils.vins.plugins import EXTRACTORS
from components.web.utils.notifications import trigger_notification
from components.web.utils.tables import table_search_helper
from components.web.utils.utils import ws_hyperscript
from components.web.utils.wrappers import acl
from dataclasses import asdict, replace
from quart import Blueprint, render_template, request, session

blueprint = Blueprint("system", __name__, url_prefix="/system")

LOG_LOCK = asyncio.Lock()
APP_LOGS_FULL_PULL = dict()
APP_LOGS_LAST_REFRESH = None

EXTRACTORS_GROUPED = dict()
for extractor in EXTRACTORS:
    for handle in extractor.handles:
        if handle not in EXTRACTORS_GROUPED:
            EXTRACTORS_GROUPED[handle] = set()
        EXTRACTORS_GROUPED[handle].add((extractor.name, extractor.priority))


@blueprint.route("/status/refresh", methods=["POST"])
@acl("system")
async def status_refresh():
    await cluster.send_command("STATUS", "*")
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
    patch_data = SystemSettingsPatch(**request.form_parsed)

    async with db:
        system_settings = await db.get("system_settings", "1")
        if system_settings:
            system_settings = SystemSettings(**system_settings)
            system_settings = replace(system_settings, **patch_data.dump_patched())
            system_settings_dict = asdict(system_settings)
            await db.patch("system_settings", "1", system_settings_dict)
        else:
            system_settings_dict = asdict(patch_data)
            await db.upsert("system_settings", "1", system_settings_dict)

    return trigger_notification(
        level="success",
        response_code=204,
        title="Settings saved",
        message="System settings were updated",
    )


@blueprint.route("/settings")
@acl("system")
async def settings():
    async with db:
        system_settings = await db.get("system_settings", "1")

    if system_settings:
        system_settings = SystemSettings(**system_settings)
    else:
        system_settings = SystemSettingsPatch()

    return await render_template(
        "system/settings.html",
        data={"settings": system_settings or {}, "extractors": EXTRACTORS_GROUPED},
    )


@blueprint.route("/logs")
@blueprint.route("/logs/search", methods=["POST"])
@acl("system")
async def cluster_logs():
    if request.method == "POST":

        def _log_file_generator():
            yield "logs/application.log"
            for peer in cluster.peers.remotes:
                if os.path.isfile(f"logs/application.{peer}.log"):
                    yield f"logs/application.{peer}.log"

        q, page, page_size, sort_attr, sort_reverse, filters = table_search_helper(
            request.form_parsed,
            "system_logs",
            default_sort_attr="record.time.repr",
            default_sort_reverse=True,
        )
        _logs = []
        async with LOG_LOCK:
            parser_failed = False
            log_files = list(_log_file_generator())
            with fileinput.input(log_files, encoding="utf-8") as f:
                for line in f:
                    if q in line:
                        try:
                            _logs.append(json.loads(line.strip()))
                        except json.decoder.JSONDecodeError:
                            parser_failed = True
                            os.unlink(f.filename())
                            f.nextfile()

            if parser_failed:
                return (
                    "",
                    204,
                    {
                        "HX-Trigger": json.dumps(
                            {"refreshLogs": {"target": "#system-logs-refresh"}}
                        ),
                    },
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
                "total_pages": len(log_pages),
                "total": len(_logs),
            },
        )
    else:
        return await render_template("system/logs.html")


@blueprint.route("/logs/refresh")
@acl("system")
async def refresh_cluster_logs():
    async with LOG_LOCK:
        for peer in cluster.peers.get_established():
            startb = -1
            file_path = f"logs/application.{peer}.log"
            if os.path.exists(file_path) and os.path.getsize(file_path) > (
                5 * 1024 * 1024
            ):
                startb = 0

            try:
                await cluster.files.fileget(
                    "logs/application.log",
                    f"logs/application.{peer}.log",
                    peer,
                    startb,
                    -1,
                )
            except Exception as e:
                if str(e).endswith("START_BEHIND_FILE_END"):
                    await cluster.files.fileget(
                        "logs/application.log", f"logs/application.{peer}.log", peer
                    )

        if cluster.peers.get_offline_peers():
            await ws_hyperscript(
                session["login"],
                """trigger notification(
                    title: 'Offline peers',
                    level: 'warning',
                    message: 'One or more peers seem to be offline and were not pulled',
                    duration: 3000
                ) on body""",
                "/system/logs",
            )

    return (
        "",
        204,
        {
            "HX-Trigger": json.dumps(
                {"logsReady": {"target": "#system-logs-table-search"}}
            ),
        },
    )
