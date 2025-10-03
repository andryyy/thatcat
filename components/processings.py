import asyncio

from components.cluster import cluster
from components.database import db
from components.logs import logger
from components.models.assets import Asset
from components.models.processings import ProcessingAdd, uuid4
from components.models.system import SystemSettings
from components.utils import ImageExif, UnidentifiedImageError, VinTool
from components.web.utils.quart import session
from dataclasses import asdict

processing_limiter = asyncio.Semaphore(3)


async def process_image(image_bytes, image_filename):
    async with db:
        settings = await db.get("system_settings", "1")
        settings = SystemSettings(**settings)

    if not settings.GOOGLE_VISION_API_KEY:
        raise ValueError("No Google Vision API key found in settings")

    async with processing_limiter:
        try:
            image_exif = ImageExif(image_bytes)
            location = {}
            location["lat"], location["lon"] = image_exif.lat_lon
        except Exception as e:
            logger.warning(e)
            location = None
        except UnidentifiedImageError as e:
            logger.critical(e)
            return

        try:
            vins = await VinTool.text_detection(
                settings.GOOGLE_VISION_API_KEY, image_bytes
            ) or [None]
        except Exception as e:
            logger.error(f"VIN text extraction error for file {image_filename}: {e}")
            vins = [None]

        try:
            async with db:
                for vin in vins:
                    asset = await Asset.create_from_bytes(
                        image_bytes, filename=image_filename
                    )
                    processing_data = ProcessingAdd(
                        **{
                            "assets": [asset],
                            "location": location,
                            "vin": vin,
                            "assigned_user": session["id"],
                        }
                    )
                    await db.upsert(
                        "processings",
                        processing_data.id,
                        asdict(processing_data),
                    )

        except Exception as e:
            logger.critical(e)
