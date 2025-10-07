from components.models.assets import Asset
from components.models.coords import Location
from components.models.helpers import *
from components.utils.datetimes import utc_now_as_str
from dataclasses import dataclass, field, asdict
from uuid import uuid4


@dataclass
class ProcessingBase:
    id: str
    created: str
    doc_version: int | str


@dataclass
class ProcessingData:
    assigned_user: str
    assets: list[Asset | dict | None] | str = field(default_factory=list)
    location: Location | dict | None = None
    vin: str | None = None


@dataclass
class Processing(ProcessingData, ProcessingBase):
    def __post_init__(self) -> None:
        self.id = validate_uuid_str(self.id)
        self.doc_version = to_int(self.doc_version)

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
            if not VinTool.verify_checksum(self.vin):
                raise ValueError("vin", "'vin' is not a valid VIN")

        if self.assets:
            if isinstance(self.assets, str):
                self.assets = to_asset_from_str(self.assets)
            elif not isinstance(self.assets, list):
                raise TypeError(
                    "assets",
                    f"'assets' must be list or JSON string, got {type(self.assets).__name__}",
                )
            else:
                assets = []
                for a in self.assets:
                    if isinstance(a, dict):
                        assets.append(Asset(**a))
                    elif isinstance(a, Asset):
                        assets.append(a)
                    elif isinstance(a, str):
                        assets.append(to_asset_from_str(a))
                    else:
                        raise TypeError(
                            "assets",
                            "All items in 'assets' must be of type Asset, dict or JSON string",
                        )
                self.assets = assets


@dataclass
class ProcessingAdd(ProcessingData):
    id: str = field(default_factory=lambda: str(uuid4()), init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)
    doc_version: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        Processing(**asdict(self))
