import mimetypes

from .models import DataType
from .plugins import EXTRACTORS
from .plugins.base import VINExtractorPlugin
from components.logs import logger
from magic import Magic


def _mime_type_from_bytes(data_bytes: bytes) -> str:
    mime = Magic(mime=True)
    return mime.from_buffer(data_bytes)


def _mime_type_from_filename(filename: str) -> str:
    mime, _ = mimetypes.guess_file_type(filename)
    return mime


def _mime_to_datatype(mime_type: str) -> DataType:
    if mime_type.startswith("image/"):
        return DataType.IMAGE
    else:
        return DataType.DOCUMENT


class VINExtractor:
    @staticmethod
    async def get_extractor_for_mime(mime_type: str) -> VINExtractorPlugin | None:
        from components.database import db
        from components.models.system import SystemSettings

        async with db:
            settings = await db.get("system_settings", "1")
            settings = SystemSettings(**settings)

        candidates = []
        required_type = _mime_to_datatype(mime_type)

        for extractor in EXTRACTORS:
            if required_type in extractor.handles:
                priority_override = settings.extractor_priority_overrides.get(
                    required_type.value, {}
                ).get(extractor.name.replace(".", ""), None)

                if priority_override is not None:
                    extractor.priority = priority_override

                candidates.append(extractor)

        if candidates:
            candidates.sort(key=lambda e: e.priority)
            for candidate in candidates:
                try:
                    c = candidate(settings)
                    logger.info(
                        f"Using {candidate.name} ({candidate.priority}) for type {required_type.value}"
                    )
                    return c
                except Exception as e:
                    logger.warning(f"Cannot initialize {candidate.name!r}: {e}")

        return None

    @staticmethod
    async def get_extractor_for_bytes(data_bytes: bytes) -> VINExtractorPlugin | None:
        mime_type = _mime_type_from_bytes(data_bytes)
        return await VINExtractor.get_extractor_for_mime(mime_type)

    @staticmethod
    async def get_extractor_for_filename(filename: str) -> VINExtractorPlugin | None:
        mime_type = _mime_type_from_filename(filename)
        return await VINExtractor.get_extractor_for_mime(mime_type)


__all__ = ["VINExtractor"]
