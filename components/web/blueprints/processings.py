import asyncio
import json
import os
import components.processings
from components.models.processings import CompleteProcessingRequest
from components.utils.assets import remove_asset
from components.web.utils import *

blueprint = Blueprint("processings", __name__, url_prefix="/processings")


@blueprint.route("/", methods=["GET"])
@acl(["user"])
@formoptions(["projects"])
async def get_incomplete():
    return await render_template(
        "processings/processings.html", processings=await components.processings.all()
    )


@blueprint.route("/processing/<processing_id>", methods=["GET"])
@acl(["user"])
@formoptions(["projects"])
async def get_processing(processing_id):
    processing = await components.processings.get(processing_id)
    if not processing:
        return trigger_notification(
            level="error",
            response_code=404,
            title="Vorgang unbekannt",
            message=f"Vorgang {processing_id} nicht gefunden",
        )

    return await render_template(
        "processings/processings.html", processings=[processing]
    )


@blueprint.route("/processing/finalize", methods=["POST"])
@acl(["user"])
async def finalize_processing():
    processing_data = CompleteProcessingRequest.model_validate(request.form_parsed)
    processing_item = await components.processings.get(processing_data.id)

    if not processing_item:
        return trigger_notification(
            level="error",
            response_code=404,
            title="Vorgang unbekannt",
            message=f"Vorgang {processing_id} nicht gefunden",
        )

    async with ClusterLock("processing"):
        await components.processings.delete(processing_data.id)

    if processing_data.reason == "abort":
        for asset in processing_item["assets"]:
            await remove_asset(asset["id"])

        return trigger_notification(
            level="success",
            response_code=204,
            title="Verarbeitung abgebrochen",
            message="Das Objekt wurde verworfen",
            additional_triggers={"removeProcessing": processing_data.id},
        )

    elif processing_data.reason == "completed":
        return (
            "",
            204,
            {"HX-Trigger": json.dumps({"removeProcessing": processing_data.id})},
        )


@blueprint.route("/upload/process", methods=["POST"])
@acl(["user"])
@formoptions(["projects"])
async def process_upload():
    files = await request.files
    image_files = files.getlist("images")
    data_files = files.getlist("files")

    if not session["id"] in IN_MEMORY_DB["QUEUED_USER_TASKS"]:
        IN_MEMORY_DB["QUEUED_USER_TASKS"][session["id"]] = set()

    QUEUED_USER_TASKS = IN_MEMORY_DB["QUEUED_USER_TASKS"][session["id"]]

    if not image_files and not data_files:
        return validation_error(
            [
                {
                    "loc": ["images", "files"],
                    "msg": "Keine Daten zum Verarbeiten hochgeladen",
                }
            ]
        )

    for file in image_files:
        if not file.content_type.startswith("image/"):
            continue

        t = asyncio.create_task(components.processings.process_image(file))
        QUEUED_USER_TASKS.add(t)
        t.add_done_callback(QUEUED_USER_TASKS.discard)

    return await render_template("processings/tasks.html", tasks=QUEUED_USER_TASKS)


@blueprint.route("/upload")
@acl(["user"])
async def upload():
    return await render_template("processings/upload.html", data={})


@blueprint.route("/tasks")
@acl(["user"])
async def tasks():
    if not session["id"] in IN_MEMORY_DB["QUEUED_USER_TASKS"]:
        IN_MEMORY_DB["QUEUED_USER_TASKS"][session["id"]] = set()

    QUEUED_USER_TASKS = IN_MEMORY_DB["QUEUED_USER_TASKS"][session["id"]]

    return await render_template("processings/tasks.html", tasks=QUEUED_USER_TASKS)
