import asyncio
import glob
import json
import os

from components.models.assets import Asset
from components.utils.assets import request_asset
from components.utils.osm import display_name_to_location
from components.web.utils import *

blueprint = Blueprint("main", __name__, url_prefix="/")
HS_DIR = os.path.abspath("components/web/templates/_hs")


@blueprint.before_request
async def before_request():
    global L
    L = LANG[request.USER_LANG]


@blueprint.context_processor
async def load_languages():
    return {"L": L}


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
    location = await display_name_to_location(q)
    if location:
        return jsonify(location.model_dump(mode="json"))
    return jsonify()


@blueprint.route("/asset/<asset_id>")
@acl("any")
async def asset(asset_id: UUID | str):
    if await request_asset(cluster, asset_id):
        return await send_from_directory("assets/", asset_id)
    else:
        return "", 404


@blueprint.route("/logout", methods=["POST", "GET"])
async def logout():
    session_clear()
    return ("", 200, {"HX-Redirect": "/"})


@blueprint.websocket("/ws")
@websocket_acl("any")
async def ws():
    await websocket.send(
        f'<span class="no-text-decoration" data-tooltip="{L['Connected']}" id="ws-indicator" hx-swap-oob="outerHTML">🟢</span>'
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
