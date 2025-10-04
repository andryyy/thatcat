import asyncio
import json
import os
from components.processings import process_image
from ..utils import *

blueprint = Blueprint("processings", __name__, url_prefix="/processings")


@blueprint.context_processor
async def load_context():
    return {
        "QUEUED_USER_TASKS": STATE.queued_user_tasks.get(session["id"], []),
    }


@blueprint.route("/", methods=["GET"])
@acl(["user"])
@formoptions(["projects"])
async def get_incomplete():
    async with db:
        rows = await db.list_rows(
            "processings",
            page=1,
            page_size=-1,
            sort_attr="created",
            q="",
            sort_reverse=False,
            where={"assigned_user": session["id"]}
            if not "system" in session["acl"]
            else None,
        )

        rows["items"] = [
            await db.get("processings", item["id"]) for item in rows["items"]
        ]

    return await render_or_json(
        "processings/processings.html", request.headers, data=rows
    )


@blueprint.route("/processing/<processing_id>", methods=["GET"])
@acl(["user"])
@formoptions(["projects"])
async def get_processing(processing_id):
    async with db:
        processing = await db.get("processings", processing_id)
    if not processing:
        return trigger_notification(
            level="error",
            response_code=404,
            title="Processing unknown",
            message="Processing not found",
        )

    return await render_template("processings/processing.html", processing=processing)


@blueprint.route("/processing/finalize", methods=["POST"])
@acl(["user"])
async def finalize_processing():
    async with db:
        processing = await db.get("processings", request.form_parsed["id"])

    if not processing:
        return trigger_notification(
            level="error",
            response_code=404,
            title="Processing unknown",
            message="Processing not found",
        )

    processing_data = Processing(**processing)

    async with db:
        await db.delete("processings", processing_data.id)

    if request.form_parsed["reason"] == "abort":
        for asset in processing_data.assets:
            for peer in cluster.peers.get_established():
                await cluster.files.filedel(f"assets/{asset.id}", peer)
                if os.path.exists(f"assets/{asset.id}"):
                    os.remove(f"assets/{asset.id}")

        return trigger_notification(
            level="success",
            response_code=204,
            title="Completed",
            message="Processing removed",
            additional_triggers={"removeProcessing": processing_data.id},
        )

    elif request.form_parsed["reason"] == "completed":
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

    if not session["id"] in STATE.queued_user_tasks:
        STATE.queued_user_tasks[session["id"]] = set()

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

        image_bytes = file.read()
        image_filename = file.filename
        t = asyncio.create_task(process_image(image_bytes, image_filename))
        STATE.queued_user_tasks[session["id"]].add(t)
        t.add_done_callback(STATE.queued_user_tasks[session["id"]].discard)

    return await render_template("processings/tasks.html")


@blueprint.route("/upload")
@acl(["user"])
async def upload():
    return await render_template("processings/upload.html", data={})


@blueprint.route("/tasks")
@acl(["user"])
async def tasks():
    return await render_template("processings/tasks.html")
