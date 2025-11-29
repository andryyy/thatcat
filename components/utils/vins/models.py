from dataclasses import dataclass
from typing import Any
from components.models.assets import Asset
from enum import Enum
from .processor import VINProcessor


@dataclass
class VINResult:
    vin: str | None
    raw_response: str | None = None
    metadata: dict[str, Any] | None = None
    asset: Asset | None = None

    def __post_init__(self) -> None:
        if self.vin and not VINProcessor.validate(self.vin):
            raise ValueError("vin", "'vin' is not a valid VIN")

        if self.asset and not isinstance(self.asset, Asset):
            raise ValueError("asset", "'asset' is not a valid Asset")


class DataType(Enum):
    IMAGE = "image"  # Image files only (JPEG, PNG, WebP, etc.)
    DOCUMENT = "document"  # Any document (PDF, DOCX, XLSX, images, etc.)
