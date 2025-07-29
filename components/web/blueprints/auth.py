import asyncio
import components.users
import json

from base64 import b64decode, b64encode
from config import defaults
from components.models.users import (
    AuthToken,
    TokenConfirmation,
    TypeAdapter,
    UserSession,
)
from components.logs import logger
from components.utils import expire_key
from components.utils.datetimes import utc_now_as_str
from components.web.utils import *
from components.web.utils.webauthn import *
from secrets import token_urlsafe

blueprint = Blueprint("auth", __name__, url_prefix="/auth")


# A link to be sent to a user to login using webauthn authentication
# /auth/login/request/confirm/<request_token>
@blueprint.route("/login/request/confirm/<request_token>")
async def login_request_confirm(request_token: str):
    try:
        TypeAdapter(str).validate_python(request_token)
    except:
        return "", 200, {"HX-Redirect": "/"}

    token_status = IN_MEMORY_DB["TOKENS"]["LOGIN"].get(request_token, {}).get("status")

    if token_status == "awaiting":
        session["request_token"] = request_token
        requested_login = IN_MEMORY_DB["TOKENS"]["LOGIN"][request_token][
            "requested_login"
        ]

        return await render_template(
            "auth/login/request/confirm.html",
            login=requested_login,
        )

    session["request_token"] = None

    return "", 200, {"HX-Redirect": "/profile", "HX-Refresh": False}


@blueprint.route("/register/request/confirm/<login>", methods=["POST", "GET"])
@acl("system")
async def register_request_confirm_modal(login: str):
    return await render_template("auth/register/request/confirm.html")


# As shown to user that is currently logged in
# /auth/login/request/confirm/modal/<request_token>
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
            request_token in IN_MEMORY_DB["TOKENS"]["LOGIN"]
            and IN_MEMORY_DB["TOKENS"]["LOGIN"][request_token]["status"] == "awaiting"
        ):
            IN_MEMORY_DB["TOKENS"]["LOGIN"][request_token].update(
                {
                    "status": "confirmed",
                    "credential_id": "",
                }
            )
            current_app.add_background_task(
                expire_key,
                IN_MEMORY_DB["TOKENS"]["LOGIN"],
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


# An unknown user issues a login request to users that are currently logged in
# /auth/login/request/start
@blueprint.route("/login/request/start", methods=["POST"])
async def login_request_start():
    request_data = AuthToken.parse_obj(request.form_parsed)

    session_clear()

    try:
        user_id = await components.users.what_id(login=request_data.login)
        user = await components.users.get(user_id=user_id)
    except (ValidationError, ValueError):
        return validation_error([{"loc": ["login"], "msg": f"User is not available"}])

    request_token = token_urlsafe()

    IN_MEMORY_DB["TOKENS"]["LOGIN"][request_token] = {
        "intention": f"Authenticate user: {request_data.login}",
        "created": utc_now_as_str(),
        "status": "awaiting",
        "requested_login": request_data.login,
    }

    current_app.add_background_task(
        expire_key,
        IN_MEMORY_DB["TOKENS"]["LOGIN"],
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
# /auth/login/request/check/<request_token>
@blueprint.route("/login/request/check/<request_token>")
async def login_request_check(request_token: str):
    try:
        TypeAdapter(str).validate_python(request_token)
    except:
        session.clear()
        return "", 200, {"HX-Redirect": "/"}

    token_status, requested_login, credential_id = map(
        IN_MEMORY_DB["TOKENS"]["LOGIN"].get(request_token, {}).get,
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
            .dict()
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
        request_data = AuthToken.parse_obj(request.form_parsed)
        IN_MEMORY_DB["TOKENS"]["LOGIN"][request_data.token] = {
            "intention": f"Authenticate user: {request_data.login}",
            "created": utc_now_as_str(),
            "status": "awaiting",
            "login": request_data.login,
        }
        current_app.add_background_task(
            expire_key,
            IN_MEMORY_DB["TOKENS"]["LOGIN"],
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
            IN_MEMORY_DB["TOKENS"]["LOGIN"].get(request_data.token, {}).get,
            ["status", "login", "code"],
        )
        IN_MEMORY_DB["TOKENS"]["LOGIN"].pop(request_data.token, None)

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
        .dict()
        .items()
    ):
        session[k] = v

    return "", 200, {"HX-Redirect": "/profile", "HX-Refresh": False}


# Generate login options for webauthn authentication
@blueprint.route("/login/webauthn/options", methods=["POST"])
async def login_webauthn_options():
    try:
        user_id = await components.users.what_id(login=request.form_parsed.get("login"))
        user = await components.users.get(user_id=user_id)
        if not user.credentials:
            return "Keine Passkeys hinterlegt", 422
    except (ValidationError, ValueError):
        return "Benutzer nicht verfügbar", 422

    allow_credentials = [
        PublicKeyCredentialDescriptor(id=c.id) for c in user.credentials
    ]

    options = generate_authentication_options(
        rp_id=defaults.WEBAUTHN_RP_ID,
        timeout=defaults.WEBAUTHN_CHALLENGE_TIMEOUT * 1000,
        allow_credentials=allow_credentials,
        user_verification=UserVerificationRequirement.REQUIRED,
    )

    session["webauthn_challenge_id"] = token_urlsafe()

    IN_MEMORY_DB[session["webauthn_challenge_id"]] = {
        "challenge": b64encode(options.challenge),
        "login": user.login,
    }
    current_app.add_background_task(
        expire_key,
        IN_MEMORY_DB,
        session["webauthn_challenge_id"],
        defaults.WEBAUTHN_CHALLENGE_TIMEOUT,
    )

    return options_to_json(options), 200


@blueprint.route("/register/token", methods=["POST"])
async def register_token():
    try:
        request_data = AuthToken.parse_obj(request.form_parsed)
        IN_MEMORY_DB["TOKENS"]["REGISTER"][request_data.token] = {
            "intention": f"Register user: {request_data.login}",
            "created": utc_now_as_str(),
            "status": "awaiting",
            "login": request_data.login,
        }
        current_app.add_background_task(
            expire_key,
            IN_MEMORY_DB["TOKENS"]["REGISTER"],
            request_data.token,
            defaults.REGISTER_REQUEST_TIMEOUT,
        )
        await ws_htmx(
            "_system",
            "beforeend",
            f'<div id="auth-permit" hx-trigger="load" hx-get="/auth/register/request/confirm/{request_data.login}"></div>',
        )

    except ValidationError as e:
        return validation_error(e.errors())

    return await render_template(
        "auth/register/token.html",
        token=request_data.token,
    )
    return template


@blueprint.route("/register/webauthn/options", methods=["POST"])
async def register_webauthn_options():
    if "token" in request.form_parsed:
        try:
            request_data = TokenConfirmation.parse_obj(request.form_parsed)
        except ValidationError as e:
            return validation_error(e.errors())

        token_status, token_login, token_confirmation_code = map(
            IN_MEMORY_DB["TOKENS"]["REGISTER"].get(request_data.token, {}).get,
            ["status", "login", "code"],
        )
        IN_MEMORY_DB["TOKENS"]["REGISTER"].pop(request_data.token, None)

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

        exclude_credentials = []
        user_id = str(uuid4())
        login = token_login
        appending_passkey = False
    else:
        if not session.get("id"):
            return trigger_notification(
                level="error",
                response_code=409,
                title="Registration failed",
                message="Something went wrong",
            )

        user = await components.users.get(user_id=session["id"])

        exclude_credentials = [
            PublicKeyCredentialDescriptor(id=c.id) for c in user.credentials
        ]

        user_id = session["id"]
        login = session["login"]
        appending_passkey = True

    options = generate_registration_options(
        rp_name=defaults.WEBAUTHN_RP_NAME,
        rp_id=defaults.WEBAUTHN_RP_ID,
        user_id=user_id.encode("ascii"),
        timeout=defaults.WEBAUTHN_CHALLENGE_TIMEOUT * 1000,
        exclude_credentials=exclude_credentials,
        user_name=login,
        attestation=AttestationConveyancePreference.DIRECT,
        authenticator_selection=AuthenticatorSelectionCriteria(
            user_verification=UserVerificationRequirement.REQUIRED,
            resident_key=ResidentKeyRequirement.REQUIRED,
        ),
    )

    session["webauthn_challenge_id"] = token_urlsafe()

    IN_MEMORY_DB[session["webauthn_challenge_id"]] = {
        "challenge": b64encode(options.challenge),
        "login": login,
        "user_id": user_id,
        "appending_passkey": appending_passkey,
    }
    current_app.add_background_task(
        expire_key,
        IN_MEMORY_DB,
        session["webauthn_challenge_id"],
        defaults.WEBAUTHN_CHALLENGE_TIMEOUT,
    )

    return options_to_json(options), 200


@blueprint.route("/register/webauthn", methods=["POST"])
async def register_webauthn():
    json_body = await request.json

    webauthn_challenge_id = session.get("webauthn_challenge_id")
    session["webauthn_challenge_id"] = None

    challenge, login, user_id, appending_passkey = map(
        IN_MEMORY_DB.get(webauthn_challenge_id, {}).get,
        ["challenge", "login", "user_id", "appending_passkey"],
    )
    IN_MEMORY_DB.pop(webauthn_challenge_id, None)

    if not challenge:
        return trigger_notification(
            level="error",
            response_code=409,
            title="Registration session invalid",
            message="Registration session invalid",
            additional_triggers={"authRegFailed": "register"},
        )

    try:
        credential = parse_registration_credential_json(json_body)
        verification = verify_registration_response(
            credential=credential,
            expected_challenge=b64decode(challenge),
            expected_rp_id=defaults.WEBAUTHN_RP_ID,
            expected_origin=f"https://{defaults.WEBAUTHN_RP_ORIGIN}",
            require_user_verification=True,
        )
    except Exception as e:
        return trigger_notification(
            level="error",
            response_code=409,
            title="Registration failed",
            message="An error occured while verifying the credential",
            additional_triggers={"authRegFailed": "register"},
        )

    credential_data = {
        "id": verification.credential_id,
        "public_key": verification.credential_public_key,
        "sign_count": verification.sign_count,
        "transports": json_body.get("transports", []),
    }

    try:
        async with ClusterLock("users"):
            if not appending_passkey:
                user_id = await components.users.create(data={"login": login})
            await components.users.create_credential(
                user_id=user_id,
                data={
                    "id": verification.credential_id,
                    "public_key": verification.credential_public_key,
                    "sign_count": verification.sign_count,
                    "transports": json_body.get("transports", []),
                },
            )

    except Exception as e:
        return trigger_notification(
            level="error",
            response_code=409,
            title="Registration failed",
            message="An error occured verifying the registration",
            additional_triggers={"authRegFailed": "register"},
        )

    if appending_passkey:
        return trigger_notification(
            level="success",
            response_code=204,
            title="New token registered",
            message="A new token was appended to your account and can now be used to login",
            additional_triggers={"appendCompleted": ""},
        )

    return trigger_notification(
        level="success",
        response_code=204,
        title="Welcome on board 👋",
        message="Your account was created, you can now log in",
        additional_triggers={"regCompleted": login},
    )


@blueprint.route("/login/webauthn", methods=["POST"])
async def auth_login_verify():
    json_body = await request.json

    try:
        webauthn_challenge_id = session.get("webauthn_challenge_id")
        challenge, login = map(
            IN_MEMORY_DB.get(webauthn_challenge_id, {}).get,
            ["challenge", "login"],
        )
        IN_MEMORY_DB.pop(webauthn_challenge_id, None)
        session["webauthn_challenge_id"] = None

        if not all([webauthn_challenge_id, challenge, login]):
            return "Zeitlimit überschritten", 409

        auth_challenge = b64decode(challenge)

        user_id = await components.users.what_id(login=login)
        user = await components.users.get(user_id=user_id)

        credential = parse_authentication_credential_json(json_body)

        matched_user_credential = next(
            (
                c
                for c in user.credentials
                if c.id == credential.raw_id and c.active == True
            ),
            None,
        )

        if not matched_user_credential:
            return "Passkey für Login nicht zugelassen", 409

        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=auth_challenge,
            expected_rp_id=defaults.WEBAUTHN_RP_ORIGIN,
            expected_origin=f"https://{defaults.WEBAUTHN_RP_ORIGIN}",
            credential_public_key=matched_user_credential.public_key,
            credential_current_sign_count=matched_user_credential.sign_count,
            require_user_verification=True,
        )

        matched_user_credential.last_login = utc_now_as_str()
        if matched_user_credential.sign_count != 0:
            matched_user_credential.sign_count = verification.new_sign_count

        async with ClusterLock("users"):
            user_id = await components.users.what_id(login=login)
            await components.users.patch_credential(
                user_id=user_id,
                hex_id=credential.raw_id.hex(),
                data=matched_user_credential.model_dump(mode="json"),
            )

    except Exception as e:
        logger.critical(e)
        return "Unbehandelter Serverfehler", 409

    request_token = session.get("request_token")

    if request_token:
        """
        Not setting session login and id for device that is confirming the proxy authentication
        Gracing 10s for the awaiting party to catch up an almost expired key
        """
        IN_MEMORY_DB["TOKENS"]["LOGIN"][request_token].update(
            {
                "status": "confirmed",
                "credential_id": credential.raw_id.hex(),
            }
        )
        current_app.add_background_task(
            expire_key,
            IN_MEMORY_DB["TOKENS"]["LOGIN"],
            request_token,
            10,
        )
        session["request_token"] = None

        return "", 204, {"HX-Trigger": "proxyAuthSuccess"}

    for k, v in (
        UserSession(
            login=user.login,
            id=user.id,
            acl=user.acl,
            cred_id=credential.raw_id.hex(),
            lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES),
            profile=user.profile,
        )
        .dict()
        .items()
    ):
        session[k] = v

    return "", 200
