from components.models.helpers import *
from components.models.assets import Asset
from components.models.coords import Location
from components.utils import ensure_list, utc_now_as_str, VinTool, unique_list
from dataclasses import dataclass, field, fields, asdict
from quart.sessions import SecureCookieSession
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
    radius: int | str | None = 100
    notes: str | None = None


@dataclass
class ObjectCarData:
    vin: str
    assigned_users: list[str] | str
    vendor: str | None = None
    model: str | None = None
    year: int | None = None
    assigned_project: str | None = None
    location: Location | dict | None = None
    notes: str | None = None
    assets: list[Asset | dict | None] | str = field(default_factory=list)


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
        self.radius = to_int(self.radius)

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
        if not VinTool.verify_checksum(self.vin):
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


forms = {
    "projects": {
        "name": {
            "title": "Name",
            "description": "The name of the project",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "assigned_users": {
            "title": "Administrative Users",
            "description": "These users are allowed to fully administer the project.",
            "type": "users:multi",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "location": {"title": "Location", "type": "location"},
        "radius": {"title": "Location Radius", "type": "number"},
        "notes": {
            "title": "Notes",
            "description": "Additional information; free text",
            "type": "textarea",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    },
    "cars": {
        "vin": {
            "title": "VIN (Vehicle Identification Number)",
            "description": "The vehicle's identification number",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "assigned_users": {
            "title": "Administrative Users",
            "description": "These users are allowed to fully administer the car.",
            "type": "users:multi",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "vendor": {
            "title": "Manufacturer",
            "description": "The vehicle's manufacturer",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "model": {
            "title": "Model",
            "description": "The manufacturer's model designation",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "year": {
            "title": "Year of Manufacture",
            "description": "The vehicle's year of manufacture",
            "type": "number",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "assigned_project": {
            "title": "Assigned Project",
            "description": "Assign this car to a project.",
            "type": "project",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "location": {"title": "Location", "type": "location"},
        "notes": {
            "title": "Notes",
            "description": "Additional information; free text",
            "type": "textarea",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
        "assets": {
            "title": "Assets",
            "description": "Associated files",
            "type": "assets",
        },
    },
}

model_meta = {
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
    "system_fields": {
        "cars": ["assigned_users"],
        "projects": ["assigned_users"],
    },
}
