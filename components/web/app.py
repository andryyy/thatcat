import asyncio
import json
import random
import string

from .blueprints import auth, groups, objects, processings, profile, root, system, users
from components.cluster.exceptions import ClusterException
from components.database import db
from components.database.states import STATE
from components.models.forms import model_forms
from components.models.users import User
from components.utils.lang import LANG
from components.utils.misc import ensure_list
from components.web.utils.notifications import trigger_notification
from components.web.utils.utils import build_nested_dict, ws_hyperscript
from config import defaults
from quart import Quart, request, session
from dataclasses import asdict

app = Quart(
    __name__,
    static_url_path="/static",
    static_folder="static_files",
    template_folder="templates",
)

app.register_blueprint(root.blueprint)
app.register_blueprint(auth.blueprint)
app.register_blueprint(objects.blueprint)
app.register_blueprint(profile.blueprint)
app.register_blueprint(system.blueprint)
app.register_blueprint(users.blueprint)
app.register_blueprint(groups.blueprint)
app.register_blueprint(processings.blueprint)

app.config["SEND_FILE_MAX_AGE_DEFAULT"] = defaults.SEND_FILE_MAX_AGE_DEFAULT
app.config["SECRET_KEY"] = defaults.SECRET_KEY
app.config["TEMPLATES_AUTO_RELOAD"] = defaults.TEMPLATES_AUTO_RELOAD
app.config["BACKGROUND_TASK_SHUTDOWN_TIMEOUT"] = 1
app.config["SERVER_NAME"] = defaults.HOSTNAME
app.config["MOD_REQ_LIMIT"] = 10

modifying_request_limiter = asyncio.Semaphore(app.config["MOD_REQ_LIMIT"])


def generate_form_id(from_key: str, length=8):
    chars = string.ascii_lowercase + string.digits
    random_part = "".join(random.choices(chars, k=length))
    return f"form-{random_part}-{str(from_key)}"


@app.errorhandler(ValueError)
@app.errorhandler(TypeError)
@app.errorhandler(ClusterException)
async def handle_input_error(error):
    if isinstance(error, ClusterException):
        await ws_hyperscript(
            "@system",
            f"""trigger notification(
                    title: 'Cluster error',
                    level: 'system',
                    message: '{str(error)}',
                    duration: 10000
                )
            """,
        )
        return trigger_notification(
            level="error",
            response_body=""
            if not request.headers.get("Hx-Request")
            else f"Cluster error: {error}",
            response_code=503,
            title="Cluster error",
            message=str(error),
        )
    elif isinstance(error.args, tuple) and len(error.args) == 2:
        fields, message = error.args
        return trigger_notification(
            level="validationError",
            response_body=""
            if not request.headers.get("Hx-Request")
            else f"{LANG[request.USER_LANG]['Data validation failed']}\n{LANG[request.USER_LANG][message]}\n",
            response_code=422,
            title=LANG[request.USER_LANG]["Data validation failed"],
            message=LANG[request.USER_LANG][message],
            fields=ensure_list(fields),
        )
    return trigger_notification(
        level="validationError",
        response_body=""
        if not request.headers.get("Hx-Request")
        else f"{LANG[request.USER_LANG]['Data validation failed']}\n{LANG[request.USER_LANG][str(error)]}\n",
        response_code=422,
        title=LANG[request.USER_LANG]["Data validation failed"],
        message=LANG[request.USER_LANG][str(error)],
    )


@app.before_request
async def before_request():
    request.form_parsed = {}
    request.USER_LANG = (
        session.get("lang")
        or request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES)
        or "en"
    )

    if session.get("id") and session["id"] in STATE.promote_users:
        STATE.promote_users.discard(session["id"])
        async with db:
            user = await db.get("users", session["id"])
            if "system" not in ensure_list(user.get("acl", [])):
                user = User(**user)
                user.acl.append("system")
                session["acl"] = user.acl
                STATE.session_validated.update({session["id"]: user.acl})
                user_dict = asdict(user)
                try:
                    await db.patch("users", session["id"], {"acl": user_dict["acl"]})
                except Exception:
                    await ws_hyperscript(
                        session["login"],
                        """trigger notification(
                                title: 'Promotion warning',
                                level: 'warning',
                                message: 'Promotion could not be written to database',
                                duration: 10000
                        )""",
                    )

    if request.method in ["POST", "PATCH", "PUT", "DELETE"]:
        await modifying_request_limiter.acquire()
        form = await request.form
        request.form_parsed = build_nested_dict(form)


@app.teardown_request
async def teardown_request(exc):
    modifying_request_limiter.release()


@app.context_processor
def load_defaults():
    context = {
        k: v
        for k, v in defaults.__dict__.items()
        if not (k.startswith("__") or k.startswith("_"))
    }
    context["L"] = LANG[request.USER_LANG]
    context["generate_form_id"] = generate_form_id
    context["forms"] = model_forms
    return context


@app.template_filter(name="ensurelist")
def ensurelist(value):
    return ensure_list(value)


@app.template_filter(name="toprettyjson")
def to_prettyjson(value):
    return json.dumps(value, sort_keys=True, indent=2, separators=(",", ": "))
