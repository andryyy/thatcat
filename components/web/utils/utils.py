from quart import render_template, request, current_app
from components.database import db
from components.database.states import STATE
from components.models.users import USER_ACLS
from components.utils.datetimes import utc_now_as_str
from dataclasses import asdict, is_dataclass
from werkzeug.datastructures import ImmutableMultiDict
from functools import lru_cache


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


async def ws_hyperscript(channel, data, if_path: str = "", exclude_self: bool = False):
    if channel.startswith("@") and channel.lstrip("@") in USER_ACLS:
        channel = channel.removeprefix("@")
        async with db:
            users = await db.search(
                "users",
                where={
                    "acl": channel,
                },
            )
        for user in users:
            await ws_hyperscript(user["login"], data, if_path, exclude_self)

    if not STATE.ws_connections.get(channel, {}):
        STATE.ws_cache.set_and_expire(
            utc_now_as_str(),
            {"channel": channel, "data": data},
            30,
        )
    else:
        for ws, ws_data in STATE.ws_connections.get(channel, {}).items():
            if exclude_self and ws.cookies == request.cookies:
                continue
            if not if_path or ws_data.get("path", "").startswith(if_path):
                await ws.send(data)


async def render_or_json(tpl, headers, **context):
    if "application/json" in headers.get("Content-Type", ""):
        for k, v in context.items():
            if is_dataclass(v):
                return asdict(v)
            return v

    return await render_template(tpl, **context)


@lru_cache(maxsize=1024)
def route_exists(path: str) -> bool:
    url_map = current_app.url_map
    adapter = url_map.bind("localhost")
    for p in [path, path.rstrip("/"), f"{path}/"]:
        try:
            adapter.match(p)
            return True
        except Exception:
            continue
    return False
