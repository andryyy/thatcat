import asyncio
import json

from .blueprints import *
from .utils.quart import Quart, request, session
from .utils.utils import parse_form_to_dict, ws_htmx
from .utils.notifications import trigger_notification, validation_error

from components.cluster.exceptions import ClusterException
from components.database import STATE
from components.models import UUID, ValidationError
from components.logs import logger
from components.utils import deep_model_dump, ensure_list, merge_deep
from components.utils.datetimes import ntime_utc_now
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


@app.errorhandler(ValueError)
async def handle_validation_error(error):
    if isinstance(error.args, tuple):
        name, message = error.args
        return validation_error([{"loc": [name], "msg": message}])
    return validation_error([{"loc": [""], "msg": str(error)}])


@app.errorhandler(ValidationError)
async def handle_validation_error(error):
    return validation_error(error.errors())


@app.errorhandler(ClusterException)
async def handle_cluster_error(error):
    await ws_htmx(
        "system",
        "beforeend",
        """<div hidden _="on load trigger
            notification(
            title: 'Cluster error',
            level: 'system',
            message: '{error}',
            duration: 10000
            )"></div>""".format(
            error=str(error)
        ),
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
        request.form_parsed = dict()
        if form:
            for k in form:
                v = form.getlist(k)
                if len(v) == 1:
                    request.form_parsed = merge_deep(
                        request.form_parsed, parse_form_to_dict(k, v.pop())
                    )
                else:
                    request.form_parsed = merge_deep(
                        request.form_parsed, parse_form_to_dict(k, v)
                    )


@app.teardown_request
async def teardown_request(exc):
    modifying_request_limiter.release()


@app.context_processor
def load_defaults():
    _defaults = {
        k: v
        for k, v in defaults.__dict__.items()
        if not (k.startswith("__") or k.startswith("_"))
    }
    return _defaults


@app.template_filter(name="hex")
def to_hex(value):
    return value.hex()


@app.template_filter(name="ensurelist")
def ensurelist(value):
    return ensure_list(value)


@app.template_filter(name="toprettyjson")
def to_prettyjson(value):
    return json.dumps(value, sort_keys=True, indent=2, separators=(",", ": "))


@app.template_filter("tojson")
def to_json(value):
    return json.dumps(deep_model_dump(value))


@app.template_filter("touuid")
def str_to_uuid(value):
    return UUID(value)
