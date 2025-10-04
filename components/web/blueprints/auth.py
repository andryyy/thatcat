from ..utils import *
from ..utils.passkeys import *
from components.logs import logger
from components.utils import utc_now_as_str
from config import defaults
from secrets import token_urlsafe
from uuid import uuid4

blueprint = Blueprint("auth", __name__, url_prefix="/auth")


@blueprint.route("/login/request/confirm/<request_token>")
async def login_request_confirm(request_token: str):
    if not isinstance(request_token, str) or request_token == "":
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
    if not isinstance(request_token, str) or request_token == "":
        return "", 204, {"HX-Redirect": "/"}

    if request.method == "POST":
        if (
            request_token in STATE.sign_in_tokens
            and STATE.sign_in_tokens[request_token]["status"] == "awaiting"
        ):
            STATE.sign_in_tokens.set_and_expire(
                request_token,
                {
                    "status": "confirmed",
                    "credential_id": "",
                },
                5,
            )
            await ws_htmx(session["login"], "delete:#auth-login-request", "")
            return "", 204

        return trigger_notification(
            level="error",
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
    authentication = Authentication(**request.form_parsed)

    session_clear()

    async with db:
        user = await db.search(
            "users",
            {"login": authentication.login},
        )

    if not user:
        return ValueError("login", "User is not available")

    user = User(**user[0])

    request_token = token_urlsafe()

    STATE.sign_in_tokens.set_and_expire(
        request_token,
        {
            "intention": f"Authenticate {user.login}",
            "created": utc_now_as_str(),
            "status": "awaiting",
            "requested_login": user.login,
        },
        defaults.AUTH_REQUEST_TIMEOUT,
    )

    if user.profile.permit_auth_requests:
        await ws_htmx(
            user.login,
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
    token_status, requested_login, credential_id = map(
        STATE.sign_in_tokens.get(request_token, {}).get,
        ["status", "requested_login", "credential_id"],
    )

    if token_status == "confirmed":
        async with db:
            user = await db.search(
                "users",
                {"login": str(requested_login)},
            )

        if not user:
            raise ValueError("login", "User is not available")

        user = User(**user[0])

        for k, v in asdict(
            UserSession(
                login=user.login,
                id=user.id,
                acl=user.acl,
                cred_id=credential_id,
                lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES),
                profile=user.profile,
            )
        ).items():
            session[k] = v

    else:
        if token_status:
            return "", 204

    return "", 200, {"HX-Redirect": "/"}


@blueprint.route("/login/token", methods=["GET", "POST"])
async def login_token():
    if request.method == "GET":
        return await render_template("auth/login/token/token.html")

    authentication = Authentication(**request.form_parsed)

    async with db:
        user = await db.search(
            "users",
            {"login": authentication.login},
        )

    if not user:
        raise ValueError("login", "User is not available")

    user = User(**user[0])

    STATE.terminal_tokens.set_and_expire(
        authentication.token,
        {
            "intention": f"Authenticate user: {authentication.login}",
            "created": utc_now_as_str(),
            "status": "awaiting",
            "user_id": user.id,
        },
        defaults.AUTH_REQUEST_TIMEOUT,
    )

    return await render_template(
        "auth/login/token/confirm.html",
        token=authentication.token,
    )


@blueprint.route("/login/token/verify", methods=["POST"])
async def login_token_verify():
    token_confirmation = TokenConfirmation(**request.form_parsed)
    token_status, token_user_id, token_confirmation_code = map(
        STATE.terminal_tokens.get(token_confirmation.token, {}).get,
        ["status", "user_id", "code"],
    )
    STATE.terminal_tokens.pop(token_confirmation.token, None)

    if (
        token_status != "confirmed"
        or token_confirmation_code != token_confirmation.confirmation_code
    ):
        return validation_error(
            [{"loc": ["confirmation_code"], "msg": "Confirmation code is invalid"}]
        )

    async with db:
        user = await db.get("users", token_user_id)

    user = User(**user)

    for k, v in asdict(
        UserSession(
            login=user.login,
            id=user.id,
            acl=user.acl,
            cred_id=credential_id,
            lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES),
            profile=user.profile,
        )
    ).items():
        session[k] = v

    return "", 200, {"HX-Redirect": "/profile", "HX-Refresh": False}


@blueprint.route("/login/webauthn/options", methods=["POST"])
async def login_webauthn_options():
    gen_opts = generate_authentication_options(
        allowed_credentials=None,  # w resident key
    )

    STATE._challenge_options.set_and_expire(
        gen_opts["challenge"],
        gen_opts["options"],
        defaults.WEBAUTHN_CHALLENGE_TIMEOUT,
    )

    return gen_opts["options"], 200


@blueprint.route("/register/webauthn/options", methods=["POST"])
async def register_webauthn_options():
    if not "id" in session:
        request_data = Authentication(**request.form_parsed)
        async with db:
            user = await db.search("users", {"login": request_data.login})

        if user:
            raise ValueError("login", "User is not available")

        gen_opts = generate_registration_options(
            user_id=str(uuid4()),
            user_name=request_data.login,
            exclude_credentials=[],
        )
    else:
        async with db:
            user = await db.get("users", session["id"])

        user = User(**user)

        gen_opts = generate_registration_options(
            user_id=session["id"],
            user_name=session["login"],
            exclude_credentials=[bytes.fromhex(c.id) for c in user.credentials],
        )

    STATE._challenge_options.set_and_expire(
        gen_opts["challenge"],
        gen_opts["options"],
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
            return "Timeout exceeded", 409

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
            return "Unknown passkey", 409

        user = User(**user[0])

        for credential in user.credentials:
            if credential.id == credential_id.hex():
                if credential.active == False:
                    return "Passkey is disabled", 409
                matched_user_credential = credential
                break
        else:
            return "Unknown passkey", 409

        verification = verify_authentication_response(
            assertion_response=auth_response,
            expected_challenge=login_opts["challenge"],
            public_key_pem=matched_user_credential.public_key,
            prev_sign_count=matched_user_credential.sign_count,
        )

        if not user.active:
            return "User is not allowed to sign in", 409

        for credential in user.credentials:
            if credential.id == credential_id.hex():
                credential.last_login = utc_now_as_str()
                if verification["counter_supported"] != 0:
                    matched_user_credential.sign_count = verification["sign_count"]
                break

        user_dict = asdict(user)

        async with db:
            await db.patch("users", user_id, {"credentials": user_dict["credentials"]})

    except Exception as e:
        logger.error(f"Login error: {e}", exc_info=True)
        return "Login error", 409

    request_token = session.get("request_token")

    if request_token:
        STATE.sign_in_tokens.set_and_expire(
            request_token,
            {
                "status": "confirmed",
                "credential_id": credential_id.hex(),
            },
            5,
        )
        session.pop("request_token", None)
        return "", 202

    for k, v in asdict(
        UserSession(
            login=user.login,
            id=user.id,
            acl=user.acl,
            cred_id=credential_id.hex(),
            lang=request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES),
            profile=user.profile,
        )
    ).items():
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

        if session.get("id") and session["id"] != user_id:
            raise ValueError("User ID mismatch")

        verification = verify_registration_response(
            attestation_response=auth_response,
            expected_challenge=reg_opts["challenge"],
        )
    except Exception as e:
        logger.warning(f"An error occured registering a passkey: {e}")
        return trigger_notification(
            level="error",
            response_code=409,
            title="Registering passkey failed",
            message="An error occured registering the passkey",
        )

    new_credential = CredentialAdd(
        **{
            "id": verification["credential_id"],
            "public_key": verification["public_key_pem"],
            "sign_count": verification["sign_count"],
        }
    )
    async with db:
        if not session.get("id"):
            user = UserAdd(
                **{"login": reg_opts["user"]["name"], "credentials": [new_credential]}
            )
            await db.upsert("users", user_id, asdict(user))
        else:
            user = await db.get("users", user_id)
            user = User(**user)
            user.credentials.append(new_credential)
            user_dict = asdict(user)
            await db.patch("users", user_id, {"credentials": user_dict["credentials"]})

    return "", 204
