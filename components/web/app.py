import asyncio
import json
import random
import string

from .blueprints import root, auth, objects, profile, system, users, groups, processings
from quart import Quart, request, session
from components.web.utils.notifications import validation_error, trigger_notification
from components.web.utils.utils import build_nested_dict, ws_htmx
from components.database.states import STATE
from components.cluster.exceptions import ClusterException
from components.models import model_forms
from components.utils.misc import ensure_list
from components.utils.lang import LANG
from config import defaults

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
async def handle_input_error(error):
    if isinstance(error.args, tuple) and len(error.args) == 2:
        name, message = error.args
        return validation_error(
            [{"loc": (loc,), "msg": message} for loc in ensure_list(name) or "_"]
        )
    return validation_error([{"loc": [""], "msg": str(error)}])


@app.errorhandler(ClusterException)
async def handle_cluster_error(error):
    await ws_htmx(
        "system",
        "beforeend",
        f"<div hidden _=\"on load trigger notification(title: 'Cluster error', level: 'system', message: '{str(error)}', duration: 10000)\"></div>",
    )
    return trigger_notification(
        level="error",
        response_body="",
        response_code=999,
        title="Cluster error",
        message=str(error),
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
        user = await get(user_id=session["id"])
        if "system" not in user.acl:
            user.acl.append("system")
            session["acl"] = user.acl
            STATE.session_validated.update({session["id"]: user.acl})

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
