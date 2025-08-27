from components.models.users import USER_ACLS, UserProfile, ListRowUser, User
from components.utils import batch, ensure_list
from components.web.utils import *


blueprint = Blueprint("users", __name__, url_prefix="/users")


@blueprint.before_request
async def before_request():
    global L
    request.USER_LANG = (
        session.get("lang")
        or request.accept_languages.best_match(defaults.ACCEPT_LANGUAGES)
        or "en"
    )
    L = LANG[request.USER_LANG]


@blueprint.context_processor
def load_context():
    return {
        "schemas": {"user_profile": UserProfile.model_json_schema()},
        "USER_ACLS": USER_ACLS,
        "L": LANG[request.USER_LANG],
    }


@blueprint.route("/<user_id>")
@acl("system")
async def get_user(user_id: str):
    async with db:
        user = await db.get("users", user_id)
        if user:
            user = User.model_validate(user)
        else:
            if "Hx-Request" in request.headers:
                return trigger_notification(
                    level="error",
                    response_code=409,
                    title=L["User error"],
                    message=L["User is unknown"],
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
            "users/includes/table_body.html",
            request.headers,
            data={
                "users": [ListRowUser.model_validate(row) for row in rows["items"]],
                "page_size": rows["page_size"],
                "page": rows["page"],
                "pages": rows["total_pages"],
                "elements": rows["total"],
            },
        )
    else:
        return await render_template("users/users.html")


@blueprint.route("/delete", methods=["POST"])
@blueprint.route("/<user_id>", methods=["DELETE"])
@acl("system")
async def delete_user(user_id: str | None = None):
    if request.method == "POST":
        user_ids = request.form_parsed.get("id")

    async with ClusterLock("users"):
        for user_id in ensure_list(user_ids):
            await components.users.delete(user_id=user_id)

    return trigger_notification(
        level="success",
        response_code=204,
        title="User removed",
        message=f"{len(ensure_list(user_ids))} user{'s' if len(ensure_list(user_ids)) > 1 else ''} removed",
    )


@blueprint.route("/<user_id>/credential/<hex_id>", methods=["PATCH"])
@acl("any")
async def patch_user_credential(user_id: str, hex_id: str):
    if not "system" in session["acl"]:
        user_id = session["id"]

    async with ClusterLock("users"):
        await components.users.patch_credential(
            user_id=user_id,
            hex_id=hex_id,
            data=request.form_parsed,
        )

    return trigger_notification(
        level="success",
        response_code=204,
        title="Credential modified",
        message="Credential was modified",
    )


@blueprint.route("/<user_id>/credential/<hex_id>", methods=["DELETE"])
@acl("any")
async def delete_user_credential(user_id: str, hex_id: str):
    if not "system" in session["acl"]:
        user_id = session["id"]

    async with ClusterLock("users"):
        await components.users.delete_credential(
            user_id=user_id,
            hex_id=hex_id,
        )

    return trigger_notification(
        level="success",
        response_code=204,
        title="Credential deleted",
        message="Credential was removed",
    )


@blueprint.route("/patch", methods=["POST"])
@blueprint.route("/<user_id>", methods=["PATCH"])
@acl("system")
async def patch_user(user_id: str | None = None):
    if request.method == "POST":
        user_id = request.form_parsed.get("id")

    async with ClusterLock("users"):
        await components.users.patch(user_id=user_id, data=request.form_parsed)
        await components.users.patch_profile(
            user_id=user_id, data=request.form_parsed.get("profile", {})
        )

    STATE.session_validated.pop(user_id, None)

    return trigger_notification(
        level="success",
        response_code=204,
        title="User modified",
        message=f"User was updated",
    )
