import components.users

from .quart import request, render_template
from components.utils import deep_model_dump
from components.database.states import STATE


def parse_form_to_dict(key, value):
    keys = key.split(".")
    nested_dict = {}
    current_level = nested_dict

    for key in keys[:-1]:
        current_level = current_level.setdefault(key, {})

    current_level[keys[-1]] = value
    return nested_dict


async def ws_htmx(
    channel, strategy: str, data, if_path: str = "", exclude_self: bool = False
):
    from components.models.users import USER_ACLS

    if channel.startswith("_") and channel in [f"_{acl}" for acl in USER_ACLS]:
        channel = channel.removeprefix("_")
        users, _ = await components.users.search(name="", filters={"acl": channel})
        for user in [user.login for user in users]:
            await ws_htmx(user, strategy, data, if_path, exclude_self)

    for ws, ws_data in STATE.ws_connections.get(channel, {}).items():
        if exclude_self and ws.cookies == request.cookies:
            continue
        if not if_path or ws_data.get("path", "").startswith(if_path):
            await ws.send(f'<div id="ws-recv" hx-swap-oob="{strategy}">{data}</div>')


async def render_or_json(tpl, headers, **context):
    if "application/json" in headers.get("Content-Type", ""):
        converted_context = {
            key: deep_model_dump(value) for key, value in context.items()
        }
        return next(filter(lambda x: x, converted_context.values()), dict())

    return await render_template(tpl, **context)
