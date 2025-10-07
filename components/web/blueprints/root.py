import asyncio
import json
import os

from components.utils.osm import display_name_to_location, CoordsResolver
from quart import Blueprint, abort, current_app, redirect, render_template, send_from_directory, session, url_for, websocket
from components.web.utils.wrappers import acl, session_clear, websocket_acl
from components.cluster import cluster
from components.cluster.files import FileGetException
from components.database.states import STATE

blueprint = Blueprint("main", __name__, url_prefix="/")
HS_DIR = os.path.abspath("components/web/templates/_hs")


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
    return await display_name_to_location(q)


@blueprint.route("/location/resolve/<coords>")
@acl("user")
async def coords_resolver(coords: str):
    return await CoordsResolver(coords).aresolve()


@blueprint.route("/asset/<asset_id>")
@acl("any")
async def asset(asset_id: str):
    asset_path = os.path.abspath(f"assets/{asset_id}")
    if not asset_path.startswith(os.path.abspath("./assets")):
        return "", 404

    if os.path.exists(f"assets/{asset_id}"):
        return await send_from_directory("assets/", asset_id)

    for peer in cluster.peers.get_established():
        try:
            await cluster.files.fileget(
                f"assets/{asset_id}", f"assets/{asset_id}", peer
            )
            return await send_from_directory("assets/", asset_id)
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
        '<span class="no-text-decoration" id="ws-indicator" hx-swap-oob="outerHTML">🟢</span>'
    )
    data_dict = None
    while not current_app.stop_event.is_set():
        try:
            async with asyncio.timeout(5):
                data = await websocket.receive()
            data_dict = json.loads(data)
            if "path" in data_dict:
                STATE.ws_connections[session["login"]][
                    websocket._get_current_object()
                ] = {
                    "path": data_dict["path"],
                }
        except TimeoutError:
            if not data_dict:
                await websocket.close(1000)
                break
            data_dict = None
            await websocket.send("PING")
