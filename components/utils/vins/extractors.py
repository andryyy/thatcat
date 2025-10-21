import mimetypes

from .plugins import EXTRACTORS
from .plugins.base import VINExtractorPlugin
from .models import DataType
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
    def get_extractor_for_mime(mime_type: str) -> VINExtractorPlugin | None:
        candidates = []
        required_type = _mime_to_datatype(mime_type)
        for extractor in EXTRACTORS:
            if required_type in extractor.handles:
                candidates.append(extractor)

        if candidates:
            candidates.sort(key=lambda e: e.priority)
            return candidates[0]

        return None

    @staticmethod
    def get_extractor_for_bytes(data_bytes: bytes) -> VINExtractorPlugin | None:
        mime_type = _mime_type_from_bytes(data_bytes)
        return VINExtractor.get_extractor_for_mime(mime_type)

    @staticmethod
    def get_extractor_for_filename(filename: str) -> VINExtractorPlugin | None:
        mime_type = _mime_type_from_filename(filename)
        return VINExtractor.get_extractor_for_mime(mime_type)


__all__ = ["VINExtractor"]
