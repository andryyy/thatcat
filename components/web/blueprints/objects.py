from components.utils import ensure_list, unique_list
from ..utils import *

blueprint = Blueprint("objects", __name__, url_prefix="/objects")


@blueprint.before_request
@acl(["user"])
async def before_request():
    request._objects = dict()

    if "object_type" in request.view_args:
        if request.view_args["object_type"] not in model_meta["objects"]["types"]:
            if "Hx-Request" in request.headers:
                return trigger_notification(
                    level="error",
                    response_code=404,
                    title="Object error",
                    message="Object type is unknown",
                )
            abort(404)

    if "object_id" in request.view_args or request.form_parsed.get("id", []):
        object_ids = ensure_list(request.view_args.get("object_id")) + ensure_list(
            request.form_parsed.get("id")
        )
        object_type = request.view_args["object_type"]
        async with db:
            for id_ in object_ids:
                match = await db.get(object_type, id_)
                if match:
                    model = model_meta["objects"]["base"][object_type]
                    match = model(**match)
                if not match or (
                    session["id"] not in match.assigned_users
                    and "system" not in session["acl"]
                ):
                    if "Hx-Request" in request.headers:
                        return trigger_notification(
                            level="error",
                            response_code=404,
                            title="Object error",
                            message="Object is unknown",
                        )
                    abort(404)

                if not "system" in session["acl"]:
                    for f in model_meta["objects"]["system_fields"][object_type]:
                        if hasattr(match, f):
                            setattr(match, f, None)

                request._objects[id_] = match

    # Cannot be overwritten, but will throw errors if set
    request.form_parsed.pop("id", None)
    request.form_parsed.pop("updated", None)
    request.form_parsed.pop("created", None)


@blueprint.context_processor
async def load_context():
    return {
        "object_types": model_meta["objects"]["types"],
    }


@blueprint.route("/")
async def overview():
    return await render_template("objects/overview.html")


@blueprint.route("/<object_type>/<object_id>")
@formoptions(["projects", "users"])
async def get_object(object_type: str, object_id: str):
    return await render_or_json(
        "objects/object.html",
        request.headers,
        object=request._objects[object_id],
    )


@blueprint.route("/<object_type>")
@blueprint.route("/<object_type>/search", methods=["POST"])
@formoptions(["projects", "users"])
async def get_objects(object_type: str):
    if request.method == "POST":
        q, page, page_size, sort_attr, sort_reverse, filters = table_search_helper(
            request.form_parsed, object_type, default_sort_attr="name"
        )
        async with db:
            rows = await db.list_rows(
                object_type,
                page=page,
                page_size=page_size,
                sort_attr=sort_attr,
                q=q,
                any_of=[filters],
                sort_reverse=sort_reverse,
                where={"assigned_users": session["id"]}
                if not "system" in session["acl"]
                else None,
            )

        return await render_or_json(
            "objects/includes/objects/table_body.html", request.headers, data=rows
        )
    else:
        return await render_template(
            "objects/objects.html", data={"object_type": object_type}
        )


@blueprint.route("/<object_type>", methods=["POST"])
async def create_object(object_type: str):
    if object_type == "projects" and not "system" in session["acl"]:
        raise ValueError("", "Only administrators can create projects")

    request.form_parsed["assigned_users"] = session["id"]

    upsert_model = model_meta["objects"]["add"][object_type]
    upsert_data = upsert_model(**request.form_parsed)

    async with db:
        _unique_hits = await db.search(
            object_type,
            {
                f: getattr(upsert_data, f)
                for f in model_meta["objects"]["unique_fields"][object_type]
            },
        )
        if _unique_hits:
            raise ValueError("name", "Object exists")

        if hasattr(upsert_data, "assigned_project") and upsert_data.assigned_project:
            if not await db.search(
                "projects",
                where={
                    "assigned_users": session["id"],
                    "id": upsert_data.assigned_project,
                }
                if not "system" in session["acl"]
                else {"id": upsert_data.assigned_project},
            ):
                raise ValueError("assigned_project", "Project is not accessible")

        await db.upsert(object_type, upsert_data.id, asdict(upsert_data))

    return trigger_notification(
        level="success",
        response_code=204,
        title="Completed",
        message="Object created",
    )


@blueprint.route("/<object_type>/delete", methods=["POST"])
@blueprint.route("/<object_type>/<object_id>", methods=["DELETE"])
async def delete_object(object_type: str, object_id: str | None = None):
    object_ids = request._objects.keys()

    async with db:
        for id_ in object_ids:
            if object_type == "projects":
                for res in await db.search(
                    "cars",
                    where={
                        "assigned_project": id_,
                    },
                ):
                    await db.patch("cars", res["id"], {"assigned_project": None})

            await db.delete(object_type, id_)

    return trigger_notification(
        level="success",
        response_code=204,
        title="Completed",
        message="Object removed" if len(object_ids) == 1 else "Objects removed",
    )


@blueprint.route("/<object_type>/patch", methods=["POST"])
@blueprint.route("/<object_type>/<object_id>", methods=["PATCH"])
async def patch_object(object_type: str, object_id: str | None = None):
    object_ids = request._objects.keys()

    patch_model = model_meta["objects"]["patch"][object_type]
    base_model = model_meta["objects"]["base"][object_type]
    patch_data = patch_model(**request.form_parsed)

    if not "system" in session["acl"]:
        for f in model_meta["objects"]["system_fields"][object_type]:
            if hasattr(patch_data, f):
                setattr(patch_data, f, None)

    async with db:
        for id_ in object_ids:
            patched_object = replace(request._objects[id_], **patch_data.dump_patched())

            # Check for uniqueness
            unique_filters = {
                f: getattr(patched_object, f)
                for f in model_meta["objects"]["unique_fields"][object_type]
            }
            _unique_hits = await db.search(object_type, unique_filters)
            if _unique_hits and _unique_hits[0]["id"] != id_:
                raise ValueError(
                    model_meta["objects"]["unique_fields"][object_type], "Object exists"
                )

            # Check for project access
            if not "system" in session["acl"]:
                if (
                    hasattr(patch_data, "assigned_project")
                    and patch_data.assigned_project
                ):
                    if request._objects[id_].assigned_project:
                        _projects = unique_list(
                            [
                                patch_data.assigned_project,
                                request._objects[id_].assigned_project,
                            ]
                        )
                    else:
                        _projects = [patched_object.assigned_project]

                    _project_hits = await db.search(
                        "projects",
                        where={
                            "assigned_users": session["id"],
                            "id": _projects,
                        },
                    )

                    if set([p["id"] for p in _project_hits]) != set(_projects):
                        raise ValueError("name", "Project is not accessible")

            await db.patch(object_type, id_, asdict(patched_object))

    return trigger_notification(
        level="success",
        response_code=204,
        title="Completed",
        message="Object modified" if len(object_ids) == 1 else "Objects modified",
    )
