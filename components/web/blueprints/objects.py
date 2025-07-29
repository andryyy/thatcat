import components.objects
import re

from components.models.objects import model_classes
from components.utils import batch, ensure_list
from components.web.utils import *

blueprint = Blueprint("objects", __name__, url_prefix="/objects")


@blueprint.context_processor
async def load_context():
    context = dict()
    context["schemas"] = model_classes["schemas"]
    context["system_fields"] = model_classes["system_fields"]
    context["object_types"] = model_classes["types"]
    return context


@blueprint.before_request
async def objects_before_request():
    if "object_type" in request.view_args:
        if request.view_args["object_type"] not in model_classes["types"]:
            if "Hx-Request" in request.headers:
                return trigger_notification(
                    level="error",
                    response_code=409,
                    title="Object type error",
                    message="Object type is unknown",
                )


@blueprint.route("/")
@acl(["user"])
async def overview():
    return await render_template("objects/overview.html")


@blueprint.route("/<object_type>/<object_id>")
@acl(["user"])
@formoptions(["projects", "users"])
async def get_object(object_type: str, object_id: str):
    object_data = await components.objects.get(
        object_id=object_id, object_type=object_type
    )
    if not object_data:
        return trigger_notification(
            level="error",
            response_code=404,
            title="Objekt unbekannt",
            message=f"Das Objekt ist nicht bekannt",
        )

    return await render_or_json(
        "objects/object.html", request.headers, object=object_data
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

        objects, pagination = await components.objects.search(
            object_type=object_type,
            q=q,
            filters=filters,
            pagination={
                "page": page,
                "page_size": page_size,
                "sort_attr": sort_attr,
                "sort_reverse": sort_reverse,
            },
        )

        return await render_or_json(
            "objects/includes/objects/table_body.html",
            request.headers,
            data={
                "objects": objects,
                "page_size": page_size,
                "page": page,
                "pages": pagination.pages,
                "elements": pagination.elements,
            },
        )
    else:
        return await render_template(
            "objects/objects.html", data={"object_type": object_type}
        )


@blueprint.route("/<object_type>", methods=["POST"])
@acl(["user"])
async def create_object(object_type: str):
    async with ClusterLock(object_type):
        object_id = await components.objects.create(
            object_type=object_type, data=request.form_parsed
        )

    return trigger_notification(
        level="success",
        response_code=204,
        title="Object created",
        message=f"Object {object_id} created",
    )


@blueprint.route("/<object_type>/delete", methods=["POST"])
@blueprint.route("/<object_type>/<object_id>", methods=["DELETE"])
@acl(["user"])
async def delete_object(object_type: str, object_id: str | None = None):
    if request.method == "POST":
        object_id = request.form_parsed.get("id")

    object_ids = ensure_list(object_id)
    async with ClusterLock(object_type):
        deleted_objects = await components.objects.delete(
            object_id=object_ids, object_type=object_type
        )

    return trigger_notification(
        level="success",
        response_code=204,
        title="Object removed",
        message=f"{len(deleted_objects)} object{'s' if len(deleted_objects) > 1 else ''} removed",
    )


@blueprint.route("/<object_type>/patch", methods=["POST"])
@blueprint.route("/<object_type>/<object_id>", methods=["PATCH"])
@acl(["user"])
async def patch_object(object_type: str, object_id: str | None = None):
    if request.method == "POST":
        object_id = request.form_parsed.get("id")

    async with ClusterLock(object_type):
        patched_objects = await components.objects.patch(
            object_id=object_id, object_type=object_type, data=request.form_parsed
        )

    await ws_htmx(
        "_user",
        "beforeend",
        f'<div hx-trigger="load once" hx-sync="#object-details:drop" hx-target="#object-details" hx-select="#object-details" hx-select-oob="#object-name" hx-swap="outerHTML" hx-get="/objects/{object_type}/{object_id}"></div>',
        f"/objects/{object_type}/{object_id}",
        exclude_self=True,
    )

    return trigger_notification(
        level="success" if len(patched_objects) > 0 else "warning",
        response_code=204,
        title="Patch completed",
        message=f"{len(patched_objects)} object{'s' if (len(patched_objects) > 1 or len(patched_objects) == 0) else ''} modified",
    )
