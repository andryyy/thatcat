from quart import Blueprint, abort, render_template, request, session
from components.web.utils.wrappers import acl, formoptions
from components.web.utils.notifications import trigger_notification
from components.web.utils.utils import render_or_json
from components.web.utils.tables import table_search_helper
from components.database import db
from components.database.states import STATE
from components.models.users import USER_ACLS, User, UserPatch, CredentialPatch
from components.utils.misc import ensure_list
from dataclasses import asdict, replace


blueprint = Blueprint("users", __name__, url_prefix="/users")


@blueprint.context_processor
def load_context():
    return {
        "USER_ACLS": USER_ACLS,
    }


@blueprint.route("/<user_id>")
@acl("system")
async def get_user(user_id: str):
    async with db:
        user = await db.get("users", user_id)
        if user:
            user = User(**user)
        else:
            if "Hx-Request" in request.headers:
                return trigger_notification(
                    level="error",
                    response_code=404,
                    title="User error",
                    message="User is unknown",
                )
            abort(404)

    return await render_or_json("users/user.html", request.headers, user=user)


@blueprint.route("/")
@blueprint.route("/search", methods=["POST"])
@acl("system")
@formoptions(["users"])
async def get_users():
    if request.method == "POST":
        q, page, page_size, sort_attr, sort_reverse, filters = table_search_helper(
            request.form_parsed, "users", default_sort_attr="login"
        )

        async with db:
            rows = await db.list_rows(
                "users",
                page=page,
                page_size=page_size,
                sort_attr=sort_attr,
                q=q,
                any_of=[filters],
                sort_reverse=sort_reverse,
            )

        return await render_or_json(
            "users/includes/table_body.html", request.headers, data=rows
        )
    else:
        return await render_template("users/users.html")


@blueprint.route("/delete", methods=["POST"])
@blueprint.route("/<user_id>", methods=["DELETE"])
@acl("system")
async def delete_user(user_id: str | None = None):
    if request.method == "POST":
        user_ids = request.form_parsed.get("id")

    async with db:
        delete_ids = set()
        for user_id in ensure_list(user_ids):
            if user_id == session["id"]:
                raise ValueError("user_id", "Cannot remove this user")
            if not db.get("users", user_id):
                raise ValueError("user_id", "User is not available")
            delete_ids.add(user_id)
        for delete_id in delete_ids:
            await db.delete("users", delete_id)

    return trigger_notification(
        level="success",
        response_code=204,
        title="Completed",
        message="User removed" if len(delete_ids) == 1 else "Users removed",
    )


@blueprint.route("/<user_id>/credential/<hex_id>", methods=["PATCH"])
@acl("any")
async def patch_user_credential(user_id: str, hex_id: str):
    if "system" not in session["acl"]:
        user_id = session["id"]

    async with db:
        user = await db.get("users", user_id)
        if user:
            user = User(**user)
        else:
            if "Hx-Request" in request.headers:
                return trigger_notification(
                    level="error",
                    response_code=404,
                    title="User error",
                    message="User is unknown",
                )
            abort(404)

        for credential in user.credentials:
            if credential.id == hex_id:
                matched_user_credential = credential
                break
        else:
            raise ValueError("hex_id", "Unknown passkey")

        user.credentials.remove(matched_user_credential)
        patch_data = CredentialPatch(**request.form_parsed)
        patched_credential = replace(
            matched_user_credential, **patch_data.dump_patched()
        )
        user.credentials.append(patched_credential)
        user_dict = asdict(user)

        await db.patch("users", user_id, {"credentials": user_dict["credentials"]})

    return trigger_notification(
        level="success",
        response_code=204,
        title="Completed",
        message="Passkey modified",
    )


@blueprint.route("/<user_id>/credential/<hex_id>", methods=["DELETE"])
@acl("any")
async def delete_user_credential(user_id: str, hex_id: str):
    if "system" not in session["acl"]:
        user_id = session["id"]

    async with db:
        user = await db.get("users", user_id)
        if user:
            user = User(**user)
        else:
            if "Hx-Request" in request.headers:
                return trigger_notification(
                    level="error",
                    response_code=404,
                    title="User error",
                    message="User is unknown",
                )
            abort(404)

        user.credentials = [c for c in user.credentials if c.id != hex_id]
        user_dict = asdict(user)

        await db.patch("users", user_id, {"credentials": user_dict["credentials"]})

    return trigger_notification(
        level="success",
        response_code=204,
        title="Completed",
        message="Passkey removed",
    )


@blueprint.route("/patch", methods=["POST"])
@blueprint.route("/<user_id>", methods=["PATCH"])
@acl("system")
async def patch_user(user_id: str | None = None):
    if request.method == "POST":
        user_id = request.form_parsed.get("id")

    async with db:
        user = await db.get("users", user_id)
        user = User(**user)

        patch_data = UserPatch(**request.form_parsed)

        user = replace(user, **patch_data.dump_patched())
        user_dict = asdict(user)

        await db.patch("users", user_id, user_dict)

    STATE.session_validated.pop(user_id, None)

    return trigger_notification(
        level="success",
        response_code=204,
        title="User modified",
        message="User was updated",
    )
