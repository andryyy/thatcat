import asyncio
from components.database import *
from components.cluster.locking import ClusterLock
from components.models.assets import Asset
from components.models.coords import Location
from components.models.processings import ValidationError, validate_call, UUID, uuid4
from components.logs import logger
from components.utils.assets import remove_asset, push_asset
from components.utils.images import (
    convert_file_to_webp,
    ImageExif,
    UnidentifiedImageError,
)
from components.utils.osm import coords_to_display_name
from components.models.processings import CreateProcessingData
from components.utils.vins import VinTool
from components.web.utils.quart import current_app, session


def session_context(fn):
    async def inner(*args, **kwargs):
        if current_app and session:
            kwargs["session_context"] = session["id"], session["acl"]
        return await fn(*args, **kwargs)

    return inner


@session_context
@validate_call
async def get(processing_id: UUID, session_context: tuple = ()):
    query = Q.id == str(processing_id)

    if session_context:
        user_id, user_acl = session_context
        if not "system" in user_acl:
            query &= Q.user == str(user_id)

    async with TinyDB(**dbparams()) as db:
        return db.table("processing").get(query)


@session_context
@validate_call
async def all(session_context: tuple = ()):
    query = Q.id.exists()

    if session_context:
        user_id, user_acl = session_context
        if not "system" in user_acl:
            query &= Q.user == str(user_id)

    async with TinyDB(**dbparams()) as db:
        return db.table("processing").search(query)


async def process_image(image_file):
    image_bytes = image_file.read()
    image_filename = image_file.filename
    try:
        image_exif = ImageExif(image_bytes)
        image_coords = image_exif.coords
        location = Location.from_coords(image_coords)
        location.display_name = await coords_to_display_name(image_coords)
    except (ValueError, ValidationError) as e:
        location = None
    except UnidentifiedImageError:
        return

    try:
        vins = await VinTool.extract_from_bytes(image_bytes) or [None]
    except Exception as e:
        vins = [None]

    asset_id = str(uuid4())

    with open(f"assets/{asset_id}", "wb") as f:
        f.write(image_bytes)

    convert_file_to_webp(f"assets/{asset_id}")

    asset = Asset(
        id=asset_id,
        filename=image_filename,
    )

    try:
        await push_asset(asset_id)
        async with ClusterLock("processing"):
            async with TinyDB(**dbparams()) as db:
                for vin in vins:
                    create_processing = CreateProcessingData.model_validate(
                        {
                            "assets": [asset],
                            "location": location,
                            "vin": vin,
                            "filename": asset.filename,
                            "user": session["id"],
                        }
                    )
                    db.table("processing").insert(
                        create_processing.model_dump(mode="json")
                    )

    except Exception as e:
        logger.critical(e)
        await remove_asset(asset_id)


@session_context
@validate_call
async def delete(
    processing_id: UUID, delete_asset: bool = True, session_context: tuple = ()
):
    query = Q.id == str(processing_id)

    if session_context:
        user_id, user_acl = session_context
        if not "system" in user_acl:
            query &= Q.user == str(user_id)

    async with TinyDB(**dbparams()) as db:
        return db.table("processing").remove(query)
        return processing_id
