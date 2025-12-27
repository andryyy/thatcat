from quart import Blueprint, redirect, render_template, request, session, url_for
from components.web.utils.wrappers import acl, session_clear
from components.web.utils.notifications import trigger_notification
from components.database import db
from components.models.users import User
from components.models.profile import UserProfilePatch
from dataclasses import asdict


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
    except Exception:
        viewer_doc_version = -1

    patch_data = UserProfilePatch(**request.form_parsed)

    async with db:
        user = await db.get("users", session["id"])
        user = User(**user)
        user.profile = patch_data.merge(user.profile)
        profile_dict = asdict(user.profile)

        trigger = {}
        if session["profile"]["vault"] != profile_dict["vault"]:
            trigger = {"vaultUpdate": {"data": profile_dict["vault"]}}

        session["profile"]["vault"] = profile_dict["vault"]
        await db.patch(
            "users",
            session["id"],
            {"profile": profile_dict},
            base_version=viewer_doc_version,
        )

    session.pop("profile", None)
    session["profile"] = profile_dict

    return trigger_notification(
        level="success",
        response_code=204,
        title="Profile updated",
        message="Your profile was updated",
        additional_triggers=trigger,
    )
