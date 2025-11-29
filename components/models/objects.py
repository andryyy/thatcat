from components.models.assets import Asset
from components.models.coords import Location
from components.models.markers import CarMarker
from components.models.helpers import (
    to_assets,
    to_car_markers,
    to_bool,
    to_int,
    to_location,
    to_str,
    validate_uuid_str,
)
from components.utils.datetimes import utc_now_as_str
from components.utils.misc import ensure_list, unique_list
from dataclasses import asdict, dataclass, field, fields, replace
from typing import Protocol
from uuid import uuid4


@dataclass
class BaseObjectTemplate:
    id: str
    updated: str
    created: str
    doc_version: int | str


@dataclass
class ObjectPagination:
    page: int | str
    page_size: int | str
    sort_attr: str
    sort_reverse: bool | str
    pages: int | str = 0
    elements: int | str = 0

    def __post_init__(self) -> None:
        for name in ("page", "page_size", "pages", "elements"):
            setattr(self, name, to_int(getattr(self, name)))

        self.sort_reverse = to_bool(self.sort_reverse)

        if not isinstance(self.sort_attr, str):
            raise TypeError(
                "sort_attr",
                f"'sort_attr' must be string, got {type(self.sort_attr).__name__}",
            )


@dataclass
class ObjectProjectData:
    name: str
    assigned_users: list[str] | str
    location: Location | dict | None = None
    notes: str | None = None


@dataclass
class ObjectCarData:
    vin: str
    assigned_users: list[str] | str
    vendor: str | None = None
    model: str | None = None
    year: int | None = None
    assigned_project: str | None = None
    car_markers: list[CarMarker, dict, str] | CarMarker | dict | str = field(
        default_factory=list
    )
    location: Location | dict | None = None
    notes: str | None = None
    assets: list[Asset | dict | str | None] | Asset | dict | str | None = None


@dataclass
class ObjectAddCar(ObjectCarData):
    id: str = field(default_factory=lambda: str(uuid4()), init=False)
    updated: str = field(default_factory=utc_now_as_str, init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)
    doc_version: int = field(default=0, init=False)

    def __post_init__(self):
        ObjectCar(**asdict(self))


@dataclass
class ObjectAddProject(ObjectProjectData):
    id: str = field(default_factory=lambda: str(uuid4()), init=False)
    updated: str = field(default_factory=utc_now_as_str, init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)
    doc_version: int = field(default=0, init=False)

    def __post_init__(self):
        ObjectProject(**asdict(self))


@dataclass
class PatchTemplate:
    def merge(self, original: Protocol):
        return replace(original, **self.dump_patched())

    def dump_patched(self):
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) is not None
        }


@dataclass
class ObjectPatchCar(ObjectCarData, PatchTemplate):
    id: str = field(default=None, init=False, repr=False)
    vin: str | None = None
    assigned_users: list[str] | str | None = None
    updated: str = field(default_factory=utc_now_as_str, init=False)


@dataclass
class ObjectPatchProject(ObjectProjectData, PatchTemplate):
    id: str = field(default=None, init=False, repr=False)
    name: str | None = None
    assigned_users: str | list[str] | None = None
    updated: str = field(default_factory=utc_now_as_str, init=False)


@dataclass
class ObjectProject(ObjectProjectData, BaseObjectTemplate):
    def __post_init__(self) -> None:
        self.id = validate_uuid_str(self.id)
        self.doc_version = to_int(self.doc_version)

        if not isinstance(self.created, str) or to_str(self.created.strip()) == "":
            raise ValueError("created", "'created' must be a non-empty string")

        if not isinstance(self.updated, str) or to_str(self.updated.strip()) == "":
            raise ValueError("updated", "'updated' must be a non-empty string")

        if not isinstance(self.name, str) or to_str(self.name.strip()) == "":
            raise ValueError("'name' must be a non-empty string")

        self.assigned_users = [
            validate_uuid_str(u) for u in unique_list(ensure_list(self.assigned_users))
        ]
        if not self.assigned_users:
            raise ValueError("assigned_users", "'assigned_users' must not be empty")

        if self.location is not None:
            if isinstance(self.location, (dict, Location)):
                self.location = to_location(self.location)
            else:
                raise TypeError(
                    "location",
                    f"'location' must be Location, dict or None, got {type(self.location).__name__}",
                )

        if self.notes is not None and not isinstance(self.notes, str):
            raise TypeError(
                "notes",
                f"'notes' must be string or None, got {type(self.notes).__name__}",
            )


@dataclass
class ObjectCar(ObjectCarData, BaseObjectTemplate):
    @property
    def name(self) -> str:
        return self.vin

    def __post_init__(self) -> None:
        from components.utils.vins.processor import VINProcessor

        self.id = validate_uuid_str(self.id)
        self.doc_version = to_int(self.doc_version)
        self.year = to_int(self.year)

        if not isinstance(self.created, str) or to_str(self.created.strip()) == "":
            raise ValueError("created", "'created' must be a non-empty string")

        if not isinstance(self.updated, str) or to_str(self.updated.strip()) == "":
            raise ValueError("updated", "'updated' must be a non-empty string")

        if self.assigned_project == "":
            self.assigned_project = None
        elif self.assigned_project is not None:
            self.assigned_project = validate_uuid_str(self.assigned_project)

        self.assigned_users = [
            validate_uuid_str(u) for u in unique_list(ensure_list(self.assigned_users))
        ]
        if not self.assigned_users:
            raise ValueError("assigned_users", "'assigned_users' must not be empty")

        if not isinstance(self.vin, str) or to_str(self.vin.strip()) == "":
            raise TypeError(
                "vin",
                f"'vin' must be non-empty string, got {type(self.vin).__name__}",
            )
        self.vin = to_str(self.vin.strip())
        if not VINProcessor.validate(self.vin):
            raise ValueError("vin", "'vin' is not a valid VIN")

        if self.vendor is not None and not isinstance(self.vendor, str):
            raise TypeError(
                "vendor",
                f"'vendor' must be string or None, got {type(self.vendor).__name__}",
            )

        if self.model is not None and not isinstance(self.model, str):
            raise TypeError(
                "model",
                f"'model' must be string or None, got {type(self.model).__name__}",
            )

        if self.location is not None:
            if isinstance(self.location, (dict, Location)):
                self.location = to_location(self.location)
            else:
                raise TypeError(
                    "location",
                    f"'location' must be Location, dict or None, got {type(self.location).__name__}",
                )

        if self.notes is not None and not isinstance(self.notes, str):
            raise TypeError(
                "notes",
                f"'notes' must be string or None, got {type(self.notes).__name__}",
            )

        if self.car_markers:
            self.car_markers = to_car_markers(self.car_markers)

        if self.assets:
            self.assets = to_assets(self.assets)


model_meta = {
    "objects": {
        "types": ["cars", "projects"],
        "patch": {
            "cars": ObjectPatchCar,
            "projects": ObjectPatchProject,
        },
        "add": {
            "cars": ObjectAddCar,
            "projects": ObjectAddProject,
        },
        "base": {
            "cars": ObjectCar,
            "projects": ObjectProject,
        },
        "unique_fields": {  # str only
            "cars": ["vin", "assigned_project"],
            "projects": ["name"],
        },
        "display_attr": {
            "cars": "vin",
        },
        "system_fields": {
            "cars": ["assigned_users"],
            "projects": ["assigned_users"],
        },
    }
}
