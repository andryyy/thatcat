import components.objects
import components.users

from .notifications import trigger_notification
from .quart import abort, redirect, request, session, url_for, websocket
from components.database import IN_MEMORY_DB
from components.logs import logger
from components.models import TypeAdapter, ValidationError, Literal, validate_call, UUID
from components.utils import ensure_list
from config import defaults
from functools import wraps


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
    from components.models.users import USER_ACLS

    acls = ensure_list(acl)

    if not session.get("id"):
        raise AuthException("Session ID missing")

    if not all(item in [*USER_ACLS, "any"] for item in acls):
        raise AuthException("Unknown ACL")

    for acl in acls:
        if session["id"] not in IN_MEMORY_DB["SESSION_VALIDATED"]:
            try:
                user = await components.users.get(user_id=session["id"])
                IN_MEMORY_DB["SESSION_VALIDATED"].update({session["id"]: user.acl})
                session["acl"] = user.acl
            except:
                session_clear()
                raise AuthException("User unknown")

        if acl == "any" or acl in IN_MEMORY_DB["SESSION_VALIDATED"][session["id"]]:
            break
    else:
        raise AuthException("Access denied by ACL")


async def create_session_by_token(token):
    if len(token.split(":")) != 2:
        raise AuthException("Invalid access token format")

    token_user, token_value = token.split(":")

    try:
        user_id = await components.users.what_id(login=token_user)
        user = await components.users.get(user_id=user_id)
    except:
        session_clear()
        raise AuthException("User unknown")

    if token_value not in user.profile.access_tokens:
        raise AuthException("Token unknown in user context")

    user_session = components.users.UserSession(
        login=user.login,
        id=user.id,
        acl=user.acl,
        cred_id="",
        lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES) or "en",
        profile=user.profile,
    )

    for k, v in user_session.dict().items():
        session[k] = v


def websocket_acl(acl_type):
    def check_acl(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                await verify_session(acl_type)

                if not session["login"] in IN_MEMORY_DB["WS_CONNECTIONS"]:
                    IN_MEMORY_DB["WS_CONNECTIONS"][session["login"]] = dict()

                if (
                    not websocket._get_current_object()
                    in IN_MEMORY_DB["WS_CONNECTIONS"][session["login"]]
                ):
                    IN_MEMORY_DB["WS_CONNECTIONS"][session["login"]][
                        websocket._get_current_object()
                    ] = dict()

                return await fn(*args, **kwargs)
            except AuthException as e:
                abort(401)
            finally:
                if "login" in session:
                    for ws in IN_MEMORY_DB["WS_CONNECTIONS"].get(session["login"], {}):
                        if ws == websocket._get_current_object():
                            del IN_MEMORY_DB["WS_CONNECTIONS"][session["login"]][ws]
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
                client_addr = request.headers.get(
                    "X-Forwarded-For", request.remote_addr
                )
                logger.warning(
                    f'{client_addr} - {session.get("login")}[ID={session.get("id")}] tried to access {request.path}'
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


@validate_call
def formoptions(
    options: list[Literal[*components.objects.model_classes["types"], "users"]]
):
    def inject_options(fn):
        @wraps(fn)
        async def wrapper(*args, **kwargs):
            request.form_options = dict()

            for option in options:
                if option == "users" and not "system" in session["acl"]:
                    continue

                if not option in IN_MEMORY_DB["CACHE"]["FORMS"]:
                    IN_MEMORY_DB["CACHE"]["FORMS"][option] = {}

                if option == "users":
                    if not IN_MEMORY_DB["CACHE"]["FORMS"][option]:
                        IN_MEMORY_DB["CACHE"]["FORMS"][
                            option
                        ] = await components.users.form_options()

                elif option in components.objects.model_classes["types"]:
                    if not IN_MEMORY_DB["CACHE"]["FORMS"][option]:
                        IN_MEMORY_DB["CACHE"]["FORMS"][
                            option
                        ] = await components.objects.form_options(option)

                request.form_options[option] = IN_MEMORY_DB["CACHE"]["FORMS"][
                    option
                ].copy()

                for k, v in request.form_options[option].items():
                    if UUID(session["id"]) in v.get("assigned_users", []):
                        request.form_options[option][k]["permitted"] = True
                    else:
                        request.form_options[option][k]["permitted"] = False

            return await fn(*args, **kwargs)

        return wrapper

    return inject_options
