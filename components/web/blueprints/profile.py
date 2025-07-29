import components.users

from components.models.users import UserProfile
from components.web.utils import *

blueprint = Blueprint("profile", __name__, url_prefix="/profile")


@blueprint.context_processor
def load_context():
    context = {"schemas": {"user_profile": UserProfile.model_json_schema()}}
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
        additional_triggers={"profileUpdate": session["profile"]["tresor"]},
    )
