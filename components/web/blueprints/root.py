import asyncio
import json
import os

from components.models.assets import Asset
from components.utils.assets import request_asset
from components.utils.osm import display_name_to_location
from components.web.utils import *

blueprint = Blueprint("main", __name__, url_prefix="/")


@blueprint.route("/")
async def root():
    if session.get("id"):
        return redirect(url_for("profile.user_profile_get"))

    session_clear()
    return await render_template("auth/authenticate.html")


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
    if await request_asset(asset_id):
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
        f'<span class="no-text-decoration" data-tooltip="Connected" id="ws-indicator" hx-swap-oob="outerHTML">🟢</span>'
    )
    data_dict = None
    while not current_app.stop_event.is_set():
        try:
            async with asyncio.timeout(5):
                data = await websocket.receive()
            data_dict = json.loads(data)
            if "path" in data_dict:
                IN_MEMORY_DB["WS_CONNECTIONS"][session["login"]][
                    websocket._get_current_object()
                ] = {
                    "path": data_dict["path"],
                }
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            if not data_dict:
                await websocket.close(1000)
                break
            data_dict = None
            await websocket.send("PING")
