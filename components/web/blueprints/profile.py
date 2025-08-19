import components.users

from components.models.users import UserProfile
from components.web.utils import *
from components.utils.files import sync_folder

blueprint = Blueprint("profile", __name__, url_prefix="/profile")


@blueprint.before_request
async def before_request():
    global L
    L = LANG[request.USER_LANG]


@blueprint.context_processor
def load_context():
    context = {
        "schemas": {"user_profile": UserProfile.model_json_schema()},
        "L": L,
    }
    return context


@blueprint.route("/")
@acl("any")
async def user_profile_get():
    try:
        user = await components.users.get(user_id=session["id"])
    except ValidationError:
        session_clear()
        return redirect(url_for("root.root"))

    return await render_template("profile/profile.html", user=user)


@blueprint.route("/edit", methods=["PATCH"])
@acl("any")
async def user_profile_patch():
    async with ClusterLock("users"):
        await components.users.patch_profile(
            user_id=session["id"], data=request.form_parsed
        )

    user = await components.users.get(user_id=session["id"])
    session.pop("profile", None)
    session["profile"] = user.profile.dict()

    return trigger_notification(
        level="success",
        response_code=204,
        title="Profile updated",
        message="Your profile was updated",
        additional_triggers={
            "profileUpdate": {"vault": session["profile"]["vault"]},
        },
    )
