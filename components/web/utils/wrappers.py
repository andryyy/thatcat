from .notifications import trigger_notification
from quart import redirect, request, session, url_for, websocket
from components.database import db
from components.database.states import STATE
from components.logs import logger
from components.models.users import User, USER_ACLS, UserSession
from components.models.objects import model_meta
from components.utils.misc import ensure_list, unique_list
from config import defaults
from functools import wraps
from dataclasses import asdict
from .cache import FORM_OPTIONS_CACHE, FORM_OPTIONS_TABLE_VERSIONS


class AuthException(Exception):
    pass


def session_clear(preserved_keys: list = []) -> None:
    if not preserved_keys:
        preserved_keys = defaults.PRESERVE_SESSION_KEYS

    restore_keys = set()
    for k in preserved_keys:
        session_key = session.get(k)
        if session_key:
            restore_keys.add(
                (k, session_key),
            )

    session.clear()

    for k in restore_keys:
        session[k[0]] = k[1]


async def verify_session(acl: str | list) -> None:
    acls = unique_list(ensure_list(acl))

    if not session.get("id"):
        raise AuthException("Session ID missing")

    if not all(item in [*USER_ACLS, "any"] for item in acls):
        raise AuthException("Unknown ACL")

    for acl in acls:
        if session["id"] not in STATE.session_validated:
            async with db:
                user = await db.get("users", session["id"])

            if not user:
                session_clear()
                raise AuthException("User unknown")

            user = User(**user)
            STATE.session_validated.update({session["id"]: user.acl})
            session["acl"] = user.acl

        if acl == "any" or acl in STATE.session_validated[session["id"]]:
            break
    else:
        raise AuthException("Access denied by ACL")


async def create_session_by_token(token):
    if len(token.split(":")) != 2:
        raise AuthException("Invalid access token format")

    token_user, token_value = token.split(":")
    token_user = str(token_user.strip())
    token_value = str(token_value.strip())

    if (
        not token_user
        or not token_value
        or len(token_user) < 3
        or len(token_value) < 16
    ):
        raise AuthException("Invalid token data")

    async with db:
        user = await db.search(
            "users",
            {"login": token_user},
        )

    if not user:
        session_clear()
        raise AuthException("User unknown")

    user = User(**user[0])

    if token_value not in user.profile.access_tokens:
        raise AuthException("Token invalid")

    for k, v in asdict(
        UserSession(
            login=user.login,
            id=user.id,
            acl=user.acl,
            cred_id="",
            lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES) or "en",
            profile=user.profile,
        )
    ).items():
        session[k] = v


def websocket_acl(acl_type):
    def check_acl(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                await verify_session(acl_type)

                if session["login"] not in STATE.ws_connections:
                    STATE.ws_connections[session["login"]] = dict()

                if (
                    websocket._get_current_object()
                    not in STATE.ws_connections[session["login"]]
                ):
                    STATE.ws_connections[session["login"]][
                        websocket._get_current_object()
                    ] = dict()

                await websocket.accept()
                return await fn(*args, **kwargs)
            except AuthException as e:
                logger.warning(
                    f"{websocket.remote_addr} - {session.get('login')}[ID={session.get('id')}] tried to access {websocket.path}: {e}"
                )
                await websocket.send("unauthorized")
                await websocket.close(1008)
            finally:
                if "login" in session:
                    for ws in STATE.ws_connections.get(session["login"], {}):
                        if ws == websocket._get_current_object():
                            del STATE.ws_connections[session["login"]][ws]
                            break

        return wrapper

    return check_acl


def acl(acl_type):
    def check_acl(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                if "x-access-token" in request.headers:
                    await create_session_by_token(request.headers["x-access-token"])

                await verify_session(acl_type)

                return await fn(*args, **kwargs)

            except AuthException as e:
                logger.warning(
                    f"{request.remote_addr} - {session.get('login')}[ID={session.get('id')}] tried to access {request.path}: {e}"
                )

                if "hx-request" in request.headers:
                    return trigger_notification(
                        level="error",
                        response_body="",
                        response_code=401,
                        title="Authentication Required",
                        message=str(e),
                    )
                else:
                    if "x-access-token" in request.headers:
                        return (f"Authentication Required\n{str(e)}\n", 401)
                    return redirect(url_for("main.root"))

        return wrapper

    return check_acl


def formoptions(options: list):
    def inject_options(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            request.form_options = dict()

            for option in options:
                if option == "users" and "system" not in session["acl"]:
                    continue

                rows = None
                table_version = db.table_version(option)
                cached_version = FORM_OPTIONS_TABLE_VERSIONS.get(option)

                if cached_version is not None and cached_version == table_version:
                    rows = FORM_OPTIONS_CACHE.get(option)

                if rows is None:
                    async with db:
                        rows_result = await db.list_rows(option, page_size=-1)
                    rows = rows_result["items"]
                    FORM_OPTIONS_CACHE[option] = rows
                    FORM_OPTIONS_TABLE_VERSIONS[option] = table_version

                if option == "users":
                    request.form_options[option] = {row["id"]: row for row in rows}

                elif option in model_meta["objects"]["types"]:
                    request.form_options[option] = {row["id"]: row for row in rows}

                    for k, v in request.form_options[option].items():
                        request.form_options[option][k]["permitted"] = False
                        if (
                            "system" in session["acl"]
                            or session["id"] in v["assigned_users"]
                        ):
                            request.form_options[option][k]["permitted"] = True

            return await fn(*args, **kwargs)

        return wrapper

    return inject_options
