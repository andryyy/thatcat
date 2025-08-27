import re

from components.models.objects import Location, UUID, model_classes
from components.utils import (
    batch,
    coords_to_display_name,
    deep_model_dump,
    ensure_list,
    ensure_unique_list,
)
from components.web.utils import *

blueprint = Blueprint("objects", __name__, url_prefix="/objects")


@blueprint.before_request
async def before_request():
    global L
    L = LANG[request.USER_LANG]

    request._objects = dict()

    if "object_type" in request.view_args:
        if request.view_args["object_type"] not in model_classes["types"]:
            if "Hx-Request" in request.headers:
                return trigger_notification(
                    level="error",
                    response_code=409,
                    title=L["Object error"],
                    message=L["Object type is unknown"],
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
                    match = model_classes["base"][object_type].model_validate(match)
                if not match or (
                    UUID(session["id"]) not in match.assigned_users
                    and "system" not in session["acl"]
                ):
                    if "Hx-Request" in request.headers:
                        return trigger_notification(
                            level="error",
                            response_code=409,
                            title=L["Object error"],
                            message=L["Object is unknown"],
                        )
                    abort(404)

                if not "system" in session["acl"]:
                    for f in model_classes["system_fields"][object_type]:
                        if hasattr(match, f):
                            setattr(match, f, None)

                request._objects[id_] = match


@blueprint.context_processor
async def load_context():
    return {
        "schemas": model_classes["schemas"],
        "object_types": model_classes["types"],
        "L": L,
    }


@blueprint.route("/")
@acl(["user"])
async def overview():
    return await render_template("objects/overview.html")


@blueprint.route("/<object_type>/<object_id>")
@acl(["user"])
@formoptions(["projects", "users"])
async def get_object(object_type: str, object_id: str):
    return await render_or_json(
        "objects/object.html",
        request.headers,
        object=request._objects[object_id],
    )


@blueprint.route("/<object_type>")
@blueprint.route("/<object_type>/search", methods=["POST"])
@acl(["user"])
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
                prefer_indexed=True,
            )

        return await render_or_json(
            "objects/includes/objects/table_body.html",
            request.headers,
            data={
                "objects": [
                    model_classes["list_row"][object_type].model_validate(row)
                    for row in rows["items"]
                ],
                "page_size": rows["page_size"],
                "page": rows["page"],
                "pages": rows["total_pages"],
                "elements": rows["total"],
            },
        )
    else:
        return await render_template(
            "objects/objects.html", data={"object_type": object_type}
        )


@blueprint.route("/<object_type>", methods=["POST"])
@acl(["user"])
async def create_object(object_type: str):
    if object_type == "projects" and not "system" in session["acl"]:
        raise ValueError("", L["Only administrators can create projects"])

    request.form_parsed["assigned_users"] = session["id"]
    upsert_data = model_classes["add"][object_type].model_validate(request.form_parsed)

    async with db:
        _unique_hits = await db.search(
            object_type,
            {
                f: str(getattr(upsert_data, f))
                for f in model_classes["unique_fields"][object_type]
            },
        )
        if _unique_hits:
            raise ValueError("name", L["Object exists"])

        if hasattr(upsert_data, "assigned_project"):
            if not await db.search(
                "projects",
                where={
                    "assigned_users": session["id"],
                    "id": str(upsert_data.assigned_project),
                }
                if not "system" in session["acl"]
                else {"id": str(upsert_data.assigned_project)},
            ):
                raise ValueError("assigned_project", L["Project is not accessible"])

        a = await db.upsert(object_type, upsert_data.id, deep_model_dump(upsert_data))
        print(a)

    return trigger_notification(
        level="success",
        response_code=204,
        title=L["Completed"],
        message=L["Object created"],
    )


@blueprint.route("/<object_type>/delete", methods=["POST"])
@blueprint.route("/<object_type>/<object_id>", methods=["DELETE"])
@acl(["user"])
async def delete_object(object_type: str, object_id: str | None = None):
    if request.method == "POST":
        object_id = request.form_parsed.get("id")

    object_ids = ensure_list(object_id)
    async with db:
        for id_ in object_ids:
            await db.delete(object_type, id_)

    return trigger_notification(
        level="success",
        response_code=204,
        title=L["Completed"],
        message=L["Object removed"] if len(object_ids) == 1 else L["Objects modified"],
    )


@blueprint.route("/<object_type>/patch", methods=["POST"])
@blueprint.route("/<object_type>/<object_id>", methods=["PATCH"])
@acl(["user"])
async def patch_object(object_type: str, object_id: str | None = None):
    object_ids = request._objects.keys()
    patch_data = model_classes["patch"][object_type].model_validate(request.form_parsed)

    if not "system" in session["acl"]:
        for f in model_classes["system_fields"][object_type]:
            if hasattr(patch_data, f):
                setattr(patch_data, f, None)

    if (
        hasattr(patch_data, "location")
        and isinstance(patch_data.location, Location)
        and patch_data.location._is_valid
        and patch_data.location.display_name == patch_data.location.coords
    ):
        try:
            patch_data.location.display_name = await coords_to_display_name(
                patch_data.location.coords
            )
        except (ValueError, ValidationError) as e:
            pass

    patch_data_dict = deep_model_dump(patch_data)

    async with db:
        for id_ in object_ids:
            # Check for uniqueness
            unique_filters = {
                f: getattr(patch_data, f) or getattr(request._objects[id_], f)
                for f in model_classes["unique_fields"][object_type]
            }
            _unique_hits = await db.search(
                object_type, {k: str(v) for k, v in unique_filters.items()}
            )
            if _unique_hits and _unique_hits[0]["id"] != id_:
                raise ValueError(
                    model_classes["unique_fields"][object_type], L["Object exists"]
                )

            # Check for project access
            if hasattr(patch_data, "assigned_project"):
                _projects = ensure_unique_list(
                    [
                        str(patch_data.assigned_project),
                        str(request._objects[id_].assigned_project),
                    ]
                )
                _project_hits = await db.search(
                    "projects",
                    where={
                        "assigned_users": session["id"],
                        "id": _projects,
                    }
                    if not "system" in session["acl"]
                    else {"id": _projects},
                )

                if set([p["id"] for p in _project_hits]) != set(_projects):
                    raise ValueError("name", L["Project is not accessible"])

            await db.patch(object_type, id_, patch_data_dict)

    return trigger_notification(
        level="success",
        response_code=204,
        title=L["Completed"],
        message=L["Object modified"] if len(object_ids) == 1 else L["Objects modified"],
    )
