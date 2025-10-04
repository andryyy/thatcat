from ..utils import *


blueprint = Blueprint("profile", __name__, url_prefix="/profile")


@blueprint.route("/")
@acl("any")
async def user_profile_get():
    async with db:
        user = await db.get("users", session["id"])

    if user:
        return await render_template("profile/profile.html", user=User(**user))

    session_clear()
    return redirect(url_for("root.root"))


@blueprint.route("/edit", methods=["PATCH"])
@acl("any")
async def user_profile_patch():
    try:
        viewer_doc_version = int(request.args.get("doc_version", -1))
    except:
        viewer_doc_version = -1

    async with db:
        user = await db.get("users", session["id"])
        user = User(**user)
        patch_data = UserProfilePatch(**request.form_parsed)
        user.profile = replace(user.profile, **patch_data.dump_patched())
        user_dict = asdict(user)
        session["profile"]["vault"] = user_dict["profile"]["vault"]
        await db.patch(
            "users",
            session["id"],
            {"profile": user_dict["profile"]},
            base_version=viewer_doc_version,
        )

    session.pop("profile", None)
    session["profile"] = user_dict["profile"]

    return trigger_notification(
        level="success",
        response_code=204,
        title="Profile updated",
        message="Your profile was updated",
        additional_triggers={
            "profileUpdate": {"vault": user_dict["profile"]["vault"]},
        },
    )
