import asyncio

from components.cluster import cluster
from components.cluster.locking import ClusterLock
from components.database import *
from components.logs import logger
from components.models.assets import Asset
from components.models.coords import Location
from components.models.processings import CreateProcessingData
from components.models.processings import UUID, ValidationError, uuid4, validate_call
from components.system import get_system_settings
from components.utils.assets import push_asset, remove_asset
from components.utils.images import (
    ImageExif,
    UnidentifiedImageError,
    convert_file_to_webp,
)
from components.utils.osm import coords_to_display_name
from components.utils.vins import VinTool
from components.web.utils.quart import current_app, session

processing_limiter = asyncio.Semaphore(3)


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


async def process_image(image_bytes, image_filename):
    settings = await get_system_settings()
    if not settings.details.GOOGLE_VISION_API_KEY:
        raise ValueError("No Google Vision API key found in settings")

    async with processing_limiter:
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
            vins = await VinTool.text_detection(
                settings.details.GOOGLE_VISION_API_KEY, image_bytes
            ) or [None]
        except Exception as e:
            logger.error(f"VIN text extraction error for file {image_filename}: {e}")
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
            await push_asset(cluster, asset_id)
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
            await remove_asset(cluster, asset_id)


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
