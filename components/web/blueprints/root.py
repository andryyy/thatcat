import asyncio
import json
import os

from components.utils.osm import display_name_to_location, CoordsResolver
from quart import (
    Blueprint,
    abort,
    current_app,
    redirect,
    render_template,
    send_from_directory,
    session,
    url_for,
    websocket,
)
from components.web.utils.wrappers import acl, session_clear, websocket_acl
from components.web.utils.ratelimiter import RateLimiter
from components.cluster import cluster
from components.cluster.files import FileGetException
from components.models.assets import Asset
from components.database.states import STATE

HS_DIR = os.path.abspath("components/web/templates/_hs")
blueprint = Blueprint("main", __name__, url_prefix="/")
osm_ratelimiter = RateLimiter(rate=1, per=2)  # 1 request per 2 seconds


class TerminateWebsocketTaskGroup(Exception):
    pass


@blueprint.route("/")
async def root():
    if session.get("id"):
        return redirect(url_for("profile.user_profile_get"))

    session_clear()
    return await render_template("auth/authenticate.html")


@blueprint.route("/_hs/<script_file>")
async def hs_script(script_file: str):
    script_path = os.path.abspath(os.path.join(HS_DIR, script_file))
    if script_path.startswith(HS_DIR + os.sep) and os.path.isfile(script_path):
        return await render_template(f"_hs/{script_file}")
    return abort(404)


@blueprint.route("/location/search/<q>")
@acl("user")
async def location_lookup(q: str):
    query = {"country": None, "city": None, "street": None}
    split_q = q.split(",")
    if len(split_q) == 2:
        query["city"], query["street"] = split_q
    elif len(split_q) == 3:
        query["country"], query["city"], query["street"] = split_q
    else:
        query = q
    try:
        async with asyncio.timeout(2):
            return await display_name_to_location(query)
    except TimeoutError:
        return {}


@blueprint.route("/location/resolve/<coords>")
@acl("user")
async def coords_resolver(coords: str):
    try:
        async with asyncio.timeout(0.5):
            await osm_ratelimiter.acquire()
        return await CoordsResolver(coords).aresolve()
    except TimeoutError:
        return "", 425


@blueprint.route("/asset/<asset_id>/<asset_filename>")
@blueprint.route("/asset/<asset_id>/<asset_filename>/<attachment>")
@acl("any")
async def asset(asset_id: str, asset_filename: str, attachment: str | None = None):
    try:
        asset = Asset(id=asset_id, filename=asset_filename)
    except Exception:
        return "", 404

    if os.path.exists(f"assets/{asset.id}"):
        return await send_from_directory(
            "assets/",
            asset.id,
            as_attachment=True if attachment else False,
            attachment_filename=asset.filename,
            mimetype=asset.mime_type,
        )

    for peer in cluster.peers.get_established():
        try:
            await cluster.files.fileget(
                f"assets/{asset.id}", f"assets/{asset.id}", peer
            )
            return await send_from_directory(
                "assets/",
                asset.id,
                as_attachment=True if attachment else False,
                attachment_filename=asset.filename,
                mimetype=asset.mime_type,
            )
        except FileGetException:
            pass

    return "", 404


@blueprint.route("/logout", methods=["POST", "GET"])
async def logout():
    session_clear()
    return ("", 200, {"HX-Redirect": "/"})


@blueprint.websocket("/ws")
@websocket_acl("any")
async def ws():
    await websocket.send(
        '<span class="color-green shine" id="ws-indicator" hx-swap-oob="outerHTML">🔌</span>'
    )

    async def _shutdown_monitor():
        await current_app.stop_event.wait()
        raise TerminateWebsocketTaskGroup()

    async def _ws_handler():
        try:
            while True:
                data = await websocket.receive()
                data_dict = json.loads(data)
                if "path" in data_dict:
                    path_plain = "/"
                    path_dom = ""
                    for part in data_dict["path"].split("/"):
                        if not part:
                            continue
                        path_plain += f"{part}/"
                        path_dom += f'/<a href="{path_plain}">{part}</a>'

                    await websocket.send(
                        f'<div id="ws-recv" hx-swap-oob="innerHTML:#ws-path">{path_dom}</div>'
                    )
                    STATE.ws_connections[session["login"]][
                        websocket._get_current_object()
                    ] = {
                        "path": data_dict["path"],
                    }
        except asyncio.CancelledError:
            await websocket.close(1001)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(_shutdown_monitor())
            tg.create_task(_ws_handler())
    except* TerminateWebsocketTaskGroup:
        pass
