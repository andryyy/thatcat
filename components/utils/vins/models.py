from dataclasses import dataclass, field
from typing import Any
from components.models.assets import Asset
from enum import Enum


@dataclass
class VINResult:
    vins: list = field(default_factory=list)
    raw_response: str | None = None
    metadata: dict[str, Any] | None = None
    asset: Asset | None = None


class DataType(Enum):
    IMAGE = "image"  # Image files only (JPEG, PNG, WebP, etc.)
    DOCUMENT = "document"  # Any document (PDF, DOCX, XLSX, images, etc.)
