import asyncio
import components.users
import json

from base64 import b64decode, b64encode
from config import defaults
from components.models.users import (
    Auth,
    TokenConfirmation,
    TypeAdapter,
    UserSession,
    User,
)
from components.logs import logger
from components.utils import expire_key, deep_model_dump, utc_now_as_str
from components.web.utils import *
from components.web.utils.passkeys import *
from secrets import token_urlsafe

blueprint = Blueprint("auth", __name__, url_prefix="/auth")


@blueprint.before_request
async def before_request():
    global L
    L = LANG[request.USER_LANG]


@blueprint.context_processor
async def load_context():
    return {"L": L}


@blueprint.route("/login/request/confirm/<request_token>")
async def login_request_confirm(request_token: str):
    try:
        TypeAdapter(str).validate_python(request_token)
    except:
        return "", 200, {"HX-Redirect": "/"}

    token_status = STATE.sign_in_tokens.get(request_token, {}).get("status")

    if token_status == "awaiting":
        session["request_token"] = request_token
        requested_login = STATE.sign_in_tokens[request_token]["requested_login"]

        return await render_template(
            "auth/login/request/confirm.html",
            login=requested_login,
        )

    session["request_token"] = None

    return "", 200, {"HX-Redirect": "/profile", "HX-Refresh": False}


# As shown to user that is currently logged in
@blueprint.route(
    "/login/request/confirm/internal/<request_token>", methods=["POST", "GET"]
)
@acl("any")
async def login_request_confirm_modal(request_token: str):
    try:
        TypeAdapter(str).validate_python(request_token)
    except:
        return "", 204

    if request.method == "POST":
        if (
            request_token in STATE.sign_in_tokens
            and STATE.sign_in_tokens[request_token]["status"] == "awaiting"
        ):
            STATE.sign_in_tokens[request_token].update(
                {
                    "status": "confirmed",
                    "credential_id": "",
                }
            )
            current_app.add_background_task(
                expire_key,
                STATE.sign_in_tokens,
                request_token,
                10,
            )

            await ws_htmx(session["login"], "delete:#auth-login-request", "")

            return "", 204

        return trigger_notification(
            level="warning",
            response_code=403,
            title="Confirmation failed",
            message="Token denied",
        )

    return await render_template("auth/login/request/internal/confirm.html")


@blueprint.route("/login/request", methods=["GET"])
async def login_request():
    return await render_template("auth/login/request.html")


# An unknown user issues a login request to users that are currently logged in
# /auth/login/request/start
@blueprint.route("/login/request", methods=["POST"])
async def login_request_start():
    request_data = Auth.parse_obj(request.form_parsed)

    session_clear()

    try:
        user_id = await components.users.what_id(login=request_data.login)
        user = await components.users.get(user_id=user_id)
    except (ValidationError, ValueError):
        return validation_error([{"loc": ["login"], "msg": f"User is not available"}])

    request_token = token_urlsafe()

    STATE.sign_in_tokens[request_token] = {
        "intention": f"Authenticate user: {request_data.login}",
        "created": utc_now_as_str(),
        "status": "awaiting",
        "requested_login": request_data.login,
    }

    current_app.add_background_task(
        expire_key,
        STATE.sign_in_tokens,
        request_token,
        defaults.AUTH_REQUEST_TIMEOUT,
    )

    if user.profile.permit_auth_requests:
        await ws_htmx(
            request_data.login,
            "beforeend",
            f'<div id="auth-permit" hx-trigger="load" hx-get="/auth/login/request/confirm/internal/{request_token}"></div>',
        )

    return await render_template(
        "auth/login/request/start.html",
        data={
            "request_token": request_token,
            "request_issued_to_user": user.profile.permit_auth_requests,
        },
    )


# Polled every second by unknown user that issued a login request
@blueprint.route("/login/request/<request_token>")
async def login_request_check(request_token: str):
    try:
        TypeAdapter(str).validate_python(request_token)
    except:
        session.clear()
        return "", 200, {"HX-Redirect": "/"}

    token_status, requested_login, credential_id = map(
        STATE.sign_in_tokens.get(request_token, {}).get,
        ["status", "requested_login", "credential_id"],
    )

    if token_status == "confirmed":
        try:
            user_id = await components.users.what_id(login=requested_login)
            user = await components.users.get(user_id=user_id)
        except ValidationError as e:
            return validation_error(
                [{"loc": ["login"], "msg": f"User is not available"}]
            )

        for k, v in (
            UserSession(
                login=user.login,
                id=user.id,
                acl=user.acl,
                cred_id=credential_id,
                lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES),
                profile=user.profile,
            )
            .model_dump()
            .items()
        ):
            session[k] = v

    else:
        if token_status:
            return "", 204

    return "", 200, {"HX-Redirect": "/"}


@blueprint.route("/login/token", methods=["GET", "POST"])
async def login_token():
    if request.method == "GET":
        return await render_template("auth/login/token/token.html")

    try:
        request_data = Auth.parse_obj(request.form_parsed)
        STATE.terminal_tokens[request_data.token] = {
            "intention": f"Authenticate user: {request_data.login}",
            "created": utc_now_as_str(),
            "status": "awaiting",
            "login": request_data.login,
        }
        current_app.add_background_task(
            expire_key,
            STATE.terminal_tokens,
            request_data.token,
            120,
        )

    except ValidationError as e:
        return validation_error(e.errors())

    return await render_template(
        "auth/login/token/confirm.html",
        token=request_data.token,
    )


@blueprint.route("/login/token/verify", methods=["POST"])
async def login_token_verify():
    try:
        request_data = TokenConfirmation.parse_obj(request.form_parsed)

        token_status, token_login, token_confirmation_code = map(
            STATE.terminal_tokens.get(request_data.token, {}).get,
            ["status", "login", "code"],
        )
        STATE.terminal_tokens.pop(request_data.token, None)

        if (
            token_status != "confirmed"
            or token_confirmation_code != request_data.confirmation_code
        ):
            return validation_error(
                [
                    {
                        "loc": ["confirmation_code"],
                        "msg": "Confirmation code is invalid",
                    }
                ]
            )

        user_id = await components.users.what_id(login=token_login)
        user = await components.users.get(user_id=user_id)

    except ValidationError as e:
        return validation_error(e.errors())

    for k, v in (
        UserSession(
            login=token_login,
            id=user.id,
            acl=user.acl,
            lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES),
            profile=user.profile,
        )
        .model_dump()
        .items()
    ):
        session[k] = v

    return "", 200, {"HX-Redirect": "/profile", "HX-Refresh": False}


@blueprint.route("/login/webauthn/options", methods=["POST"])
async def login_webauthn_options():
    gen_opts = generate_authentication_options(
        allowed_credentials=None,  # w resident key
    )

    STATE._challenge_options[gen_opts["challenge"]] = gen_opts["options"]
    current_app.add_background_task(
        expire_key,
        STATE._challenge_options,
        gen_opts["challenge"],
        defaults.WEBAUTHN_CHALLENGE_TIMEOUT,
    )

    return gen_opts["options"], 200


@blueprint.route("/register/webauthn/options", methods=["POST"])
async def register_webauthn_options():
    if not "id" in session:
        request_data = Auth.parse_obj(request.form_parsed)
        gen_opts = generate_registration_options(
            user_id=str(uuid4()),
            user_name=request_data.login,
            exclude_credentials=[],
        )
    else:
        user = await components.users.get(user_id=session["id"])
        gen_opts = generate_registration_options(
            user_id=session["id"],
            user_name=session["login"],
            exclude_credentials=[c.id for c in user.credentials],
        )

    STATE._challenge_options[gen_opts["challenge"]] = gen_opts["options"]
    current_app.add_background_task(
        expire_key,
        STATE._challenge_options,
        gen_opts["challenge"],
        defaults.WEBAUTHN_CHALLENGE_TIMEOUT,
    )

    return gen_opts["options"], 200


@blueprint.route("/login/webauthn/verify", methods=["POST"])
async def auth_login_verify():
    auth_response = await request.json

    try:
        challenge_response = get_challenge_from_attestation(auth_response)
        assert challenge_response
        login_opts = STATE._challenge_options.get(challenge_response)
        STATE._challenge_options.pop(challenge_response, None)
        if not login_opts:
            return L["Timeout exceeded"], 409

        credential_id = b64url_decode(auth_response["rawId"])
        user_id = b64url_decode(auth_response["response"]["userHandle"]).decode("utf-8")

        async with db:
            user = await db.search(
                "users",
                {
                    "credentials.id": credential_id.hex(),
                    "id": user_id,
                },
            )
            if not user:
                return L["Unknown passkey"], 409

            user = User.model_validate(user[0])

        for credential in user.credentials:
            if credential.id == credential_id:
                if credential.active == False:
                    return L["Passkey is disabled"], 409
                matched_user_credential = credential
                break
        else:
            return L["Unknown passkey"], 409

        verification = verify_authentication_response(
            assertion_response=auth_response,
            expected_challenge=login_opts["challenge"],
            public_key_pem=matched_user_credential.public_key,
            prev_sign_count=matched_user_credential.sign_count,
        )

        if not user.active:
            return L["User is not allowed to login"], 409

        async with db:
            for credential in user.credentials:
                if credential.id == credential_id:
                    credential.last_login = utc_now_as_str()
                    if verification["counter_supported"] != 0:
                        matched_user_credential.sign_count = verification["sign_count"]
                    break

            user_dict = deep_model_dump(user)
            await db.patch("users", user_id, {"credentials": user_dict["credentials"]})

    except Exception as e:
        logger.critical(e)
        return "Login error", 409

    request_token = session.get("request_token")

    if request_token:
        """
        Not setting session login and id for device that is confirming the proxy authentication
        Gracing 10s for the awaiting party to catch up an almost expired key
        """
        STATE.sign_in_tokens[request_token].update(
            {
                "status": "confirmed",
                "credential_id": credential_raw_id.hex(),
            }
        )
        current_app.add_background_task(
            expire_key,
            STATE.sign_in_tokens,
            request_token,
            10,
        )
        session["request_token"] = None

        return "", 202

    for k, v in (
        UserSession(
            login=user.login,
            id=user.id,
            acl=user.acl,
            cred_id=credential_id.hex(),
            lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES),
            profile=user.profile,
        )
        .model_dump()
        .items()
    ):
        session[k] = v

    return "", 200


@blueprint.route("/register/webauthn/verify", methods=["POST"])
async def register_webauthn():
    auth_response = await request.json
    try:
        challenge_response = get_challenge_from_attestation(auth_response)
        assert challenge_response

        reg_opts = STATE._challenge_options.get(challenge_response)
        STATE._challenge_options.pop(challenge_response, None)

        if not reg_opts:
            return "Timeout exceeded", 409

        user_id = b64url_decode(reg_opts["user"]["id"]).decode("ascii")
        login = reg_opts["user"]["name"]

        if session.get("id") and session.get("id") != user_id:
            raise ValueError("User ID mismatch")

        verification = verify_registration_response(
            attestation_response=auth_response,
            expected_challenge=reg_opts["challenge"],
        )
    except Exception as e:
        logger.critical(e)
        return trigger_notification(
            level="error",
            response_code=409,
            title="Registering passkey failed",
            message="An error occured registering the passkey",
        )

    try:
        async with ClusterLock("users"):
            if not session.get("id"):
                await components.users.create(data={"id": user_id, "login": login})
            await components.users.create_credential(
                user_id=user_id,
                data={
                    "id": verification["credential_id"],
                    "public_key": verification["public_key_pem"],
                    "sign_count": verification["sign_count"],
                },
            )
    except Exception as e:
        logger.critical(e)
        return trigger_notification(
            level="error",
            response_code=409,
            title="Passkey Fehler",
            message="Es trat ein Fehler beim Hinzufügen des Passkeys auf",
        )

    return "", 204
