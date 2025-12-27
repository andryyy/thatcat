import asyncio
import json
import os
from quart import Blueprint, render_template, request, session
from components.web.utils.wrappers import acl, formoptions
from components.web.utils.notifications import trigger_notification
from components.web.utils.utils import render_or_json
from components.utils.vins.extractors import VINExtractor
from components.database import db
from components.database.states import STATE
from components.cluster import cluster
from components.models.processings import Processing, ProcessingAdd
from dataclasses import asdict

blueprint = Blueprint("processings", __name__, url_prefix="/processings")


@blueprint.context_processor
async def load_context():
    return {
        "QUEUED_USER_TASKS": STATE.queued_user_tasks.get(session["id"], []),
    }


@blueprint.route("/")
@acl(["user"])
async def upload():
    return await render_template("processings/overview.html")


@blueprint.route("/tasks")
@acl(["user"])
async def tasks():
    return await render_template(
        "processings/tasks.html", processings_count=len(db.ids("processings"))
    )


@blueprint.route("/search", methods=["POST"])
@acl(["user"])
@formoptions(["projects", "users"])
async def get_incomplete():
    async with db:
        rows = await db.list_rows(
            "processings",
            page=1,
            page_size=-1,
            sort_attr="created",
            q="",
            sort_reverse=True,
            where=(
                {"assigned_user": session["id"]}
                if "system" not in session["acl"]
                else None
            ),
        )

        rows["items"] = [
            Processing(**await db.get("processings", item["id"]))
            for item in rows["items"]
        ]

    return await render_or_json(
        "processings/processings.html", request.headers, data=rows
    )


@blueprint.route("/finalize/<processing_id>", methods=["POST"])
@acl(["user"])
async def finalize_processing(processing_id):
    async with db:
        processing = await db.get("processings", processing_id)

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
        await db.search("processings", where={"assets.id": ""})

        if request.form_parsed["reason"] == "abort":
            for asset in processing_data.assets:
                if not await db.search("processings", where={"assets.id": asset.id}):
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

    return "", 204, {"HX-Trigger": json.dumps({"removeProcessing": processing_data.id})}


@blueprint.route("/upload", methods=["POST"])
@acl(["user"])
@formoptions(["projects"])
async def process_upload():
    async def _task(file_bytes, filename):
        vin_extractor = await VINExtractor.get_extractor_for_filename(filename)
        if not vin_extractor:
            return

        results = await vin_extractor.extract(file_bytes, filename=filename)

        async with db:
            for result in results:
                processing_data = ProcessingAdd(
                    **{
                        "vin": result.vin,
                        "location": None,
                        "metadata": result.metadata,
                        "assigned_user": session["id"],
                        "assets": [result.asset] if result.asset else [],
                    }
                )
                await db.upsert(
                    "processings",
                    processing_data.id,
                    asdict(processing_data),
                )

    files = await request.files
    form = await request.form

    image_files = files.getlist("images")
    data_files = files.getlist("files")
    text_data = form.get("text_data", "").strip()

    if session["id"] not in STATE.queued_user_tasks:
        STATE.queued_user_tasks[session["id"]] = set()

    if not image_files and not data_files and not text_data:
        raise ValueError(["images", "files", "text_data"], "No files or text provided")

    # Process file uploads
    for file in image_files:
        file_bytes = file.read()
        t = asyncio.create_task(_task(file_bytes, file.filename))
        STATE.queued_user_tasks[session["id"]].add(t)
        t.add_done_callback(STATE.queued_user_tasks[session["id"]].discard)
    for file in data_files:
        file_bytes = file.read()
        t = asyncio.create_task(_task(file_bytes, file.filename))
        STATE.queued_user_tasks[session["id"]].add(t)
        t.add_done_callback(STATE.queued_user_tasks[session["id"]].discard)

    # Process text input as virtual text/plain file
    if text_data:
        text_bytes = text_data.encode("utf-8")
        t = asyncio.create_task(_task(text_bytes, "user_text.txt"))
        STATE.queued_user_tasks[session["id"]].add(t)
        t.add_done_callback(STATE.queued_user_tasks[session["id"]].discard)

    return await render_template(
        "processings/tasks.html", processings_count=len(db.ids("processings"))
    )


@blueprint.route("/item/<processing_id>", methods=["GET"])
@acl(["user"])
@formoptions(["projects", "users"])
async def get_processing(processing_id):
    async with db:
        processing = await db.get("processings", processing_id)
        processing = Processing(**processing)
    if not processing:
        return trigger_notification(
            level="error",
            response_code=404,
            title="Processing unknown",
            message="Processing not found",
        )

    return await render_template("processings/processing.html", processing=processing)
