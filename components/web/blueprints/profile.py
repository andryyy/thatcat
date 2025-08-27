from components.models.users import User, UserProfile, UserProfilePatch
from components.web.utils import *
from components.utils import deep_model_dump

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
        async with db:
            user_doc = await db.get("users", session["id"])
            user_doc_version = await db.doc_version("users", session["id"])
            user = User.model_validate(user_doc)
            user._doc_version = user_doc_version
    except ValidationError:
        session_clear()
        return redirect(url_for("root.root"))

    return await render_template("profile/profile.html", user=user)


@blueprint.route("/edit", methods=["PATCH"])
@acl("any")
async def user_profile_patch():
    try:
        viewer_doc_version = int(request.args.get("doc_version", -1))
    except:
        viewer_doc_version = -1

    async with db:
        user_profile_model = UserProfilePatch.model_validate(request.form_parsed)
        patch_data = deep_model_dump(user_profile_model)
        if viewer_doc_version != -1:
            if await db.doc_version("users", session["id"]) > viewer_doc_version:
                raise ValueError("", L["Document changed, please reload the form"])

        await db.patch("users", session["id"], {"profile": patch_data})

    session.pop("profile", None)
    session["profile"] = patch_data

    return trigger_notification(
        level="success",
        response_code=204,
        title="Profile updated",
        message="Your profile was updated",
        additional_triggers={
            "profileUpdate": {"vault": session["profile"]["vault"]},
        },
    )
