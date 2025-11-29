from components.models.assets import Asset
from components.models.coords import Location
from components.models.helpers import (
    to_str,
    validate_uuid_str,
    to_int,
    to_location,
    to_assets,
)
from components.utils.datetimes import utc_now_as_str
from dataclasses import dataclass, field, asdict
from uuid import uuid4
from typing import Any


@dataclass
class ProcessingBase:
    id: str
    created: str
    doc_version: int | str


@dataclass
class ProcessingData:
    assigned_user: str
    assets: list[Asset]
    location: Location | dict | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    vin: str | None = None


@dataclass
class Processing(ProcessingData, ProcessingBase):
    def __post_init__(self) -> None:
        from components.utils.vins.processor import VINProcessor

        self.id = validate_uuid_str(self.id)
        self.doc_version = to_int(self.doc_version)

        if not isinstance(self.metadata, dict):
            raise ValueError("metadata", "'metadata' must be a dict")

        if not isinstance(self.created, str) or to_str(self.created.strip()) == "":
            raise ValueError("created", "'created' must be a non-empty string")

        self.assigned_user = validate_uuid_str(self.assigned_user)

        if self.location is not None:
            if isinstance(self.location, (dict, Location)):
                self.location = to_location(self.location)
            else:
                raise TypeError(
                    "location",
                    f"'location' must be Location, dict or None, got {type(self.location).__name__}",
                )

        if self.vin is not None:
            if not isinstance(self.vin, str) or to_str(self.vin.strip()) == "":
                raise TypeError(
                    "vin",
                    f"'vin' must be non-empty string, got {type(self.vin).__name__}",
                )
            self.vin = to_str(self.vin.strip())
            if not VINProcessor.validate(self.vin):
                raise ValueError("vin", "'vin' is not a valid VIN")

        self.assets = to_assets(self.assets)


@dataclass
class ProcessingAdd(ProcessingData):
    id: str = field(default_factory=lambda: str(uuid4()), init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)
    doc_version: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        Processing(**asdict(self))
