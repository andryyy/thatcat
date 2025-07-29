import components.users
from components.models.users import UserGroups, UserProfile
from components.web.utils import *


blueprint = Blueprint("groups", __name__, url_prefix="/groups")


@blueprint.context_processor
def load_context():
    context = dict()
    context["schemas"] = {"user_profile": UserProfile.model_json_schema()}
    return context


@blueprint.route("/", methods=["PATCH"])
@acl("system")
async def user_group():
    request_data = UserGroups.parse_obj(request.form_parsed)

    assigned_to, _ = await components.users.search(
        name="", filters={"groups": request_data.name}
    )
    assign_to = []

    for user_id in request_data.members:
        assign_to.append(await components.users.get(user_id=user_id))

    _all = assigned_to + assign_to

    async with ClusterLock("users"):
        for user in _all:
            if request_data.name in user.groups:
                user.groups.remove(request_data.name)

            if request_data.new_name not in user.groups and user in assign_to:
                user.groups.append(request_data.new_name)

            await components.users.patch(
                user_id=user.id, data=user.model_dump(mode="json")
            )

    return "", 204


@blueprint.route("/")
@acl("system")
@formoptions(["users"])
async def get_groups():
    return await render_template("groups/groups.html", data={})
