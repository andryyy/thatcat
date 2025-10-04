from ..utils import *


blueprint = Blueprint("groups", __name__, url_prefix="/groups")


@blueprint.route("/", methods=["PATCH"])
@acl("system")
async def user_group():
    user_groups = UserGroups(**request.form_parsed)

    new_assignments: list[dict] = []
    for user_id in user_groups.members:
        new_assignments.append(await db.get("users", user_id))

    async with db:
        current_assignments = await db.search(
            "users", where={"groups": user_groups.name}
        )

        all_assignments = current_assignments
        for assignment in new_assignments:
            if assignment not in all_assignments:
                all_assignments.append(assignment)

        for user_dict in all_assignments:
            if user_groups.name in user_dict["groups"]:
                user_dict["groups"].remove(user_groups.name)

            if (
                user_groups.new_name not in user_dict["groups"]
                and user_dict in new_assignments
            ):
                user_dict["groups"].append(user_groups.new_name)

            await db.patch("users", user_dict["id"], {"groups": user_dict["groups"]})

    return "", 204


@blueprint.route("/")
@acl("system")
async def get_groups():
    async with db:
        users = await db.search("users")
    return await render_template("groups/groups.html", users=users)
