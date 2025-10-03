from .quart import render_template, request
from components.database import db
from components.database.states import STATE
from components.models.users import USER_ACLS
from dataclasses import asdict, is_dataclass
from werkzeug.datastructures import ImmutableMultiDict


def build_nested_dict(multi_dict: ImmutableMultiDict) -> dict:
    result = {}
    for key, value in multi_dict.items(multi=True):
        *path, leaf = key.split(".")
        current = result
        for part in path:
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                current[part] = {"_value": current[part]}
            current = current[part]
        if leaf in current:
            if isinstance(current[leaf], list):
                current[leaf].append(value)
            else:
                current[leaf] = [current[leaf], value]
        else:
            current[leaf] = value
    return result


async def ws_htmx(
    channel, strategy: str, data, if_path: str = "", exclude_self: bool = False
):
    if channel.startswith("_") and channel in [f"_{acl}" for acl in USER_ACLS]:
        channel = channel.removeprefix("_")
        async with db:
            users = await db.search(
                "users",
                where={
                    "acl": channel,
                },
            )
        for user in users:
            await ws_htmx(user["login"], strategy, data, if_path, exclude_self)

    for ws, ws_data in STATE.ws_connections.get(channel, {}).items():
        if exclude_self and ws.cookies == request.cookies:
            continue
        if not if_path or ws_data.get("path", "").startswith(if_path):
            await ws.send(f'<div id="ws-recv" hx-swap-oob="{strategy}">{data}</div>')


async def render_or_json(tpl, headers, **context):
    if "application/json" in headers.get("Content-Type", ""):
        for k, v in context.items():
            if is_dataclass(v):
                return asdict(v)
            return v

        return next(filter(lambda x: x, converted_context.values()), dict())

    return await render_template(tpl, **context)
