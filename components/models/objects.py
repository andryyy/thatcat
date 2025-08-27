from components.models import *
from components.models.assets import Asset
from components.models.coords import Location
from components.utils import ensure_list, utc_now_as_str, VinTool
from quart.sessions import SecureCookieSession


class ObjectPagination(BaseModel):
    page: int
    page_size: int
    sort_attr: str
    sort_reverse: bool
    pages: int = 0
    elements: int = 0


class ObjectProjectForm(BaseModel):
    _form_id: str = PrivateAttr(default=f"form-{str(uuid4())}")

    name: constr(min_length=1) = Field(
        json_schema_extra={
            "title": "Name",
            "description": "Der Name des Projekts",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    location: Location | None = Field(
        default=None,
        json_schema_extra={
            "title": "Standort",
            "type": "location",
        },
    )

    radius: int | None = Field(
        default=100,
        json_schema_extra={
            "title": "Standort Radius",
            "type": "number",
        },
    )

    notes: str | None = Field(
        default=None,
        json_schema_extra={
            "title": "Notizen",
            "description": "Weitere Informationen; Freitext",
            "type": "textarea",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    assigned_users: list[UUID] = Field(
        json_schema_extra={
            "title": "Administrative Benutzer",
            "description": "Das Projekt darf durch diese Benutzer vollständig administriert werden.",
            "type": "users:multi",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )


class ObjectCarForm(BaseModel):
    _form_id: str = PrivateAttr(default=f"form-{str(uuid4())}")

    vin: str = Field(
        json_schema_extra={
            "title": "VIN (Fahrgestellnummer)",
            "description": "Die Fahrgestellnummer des Fahrzeugs",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    vendor: str | None = Field(
        default=None,
        json_schema_extra={
            "title": "Hersteller",
            "description": "Hersteller des Fahrzeugs",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    model: str | None = Field(
        default=None,
        json_schema_extra={
            "title": "Modell",
            "description": "Modellbezeichnung des Herstellers",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    year: int | None = Field(
        default=None,
        json_schema_extra={
            "title": "Baujahr",
            "description": "Baujahr des Fahrzeugs",
            "type": "number",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    assigned_project: UUID = Field(
        json_schema_extra={
            "title": "Zugehöriges Projekt",
            "description": "Dieses Auto einem Projekt zuordnen.",
            "type": "project",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    location: Location | None = Field(
        default=None,
        json_schema_extra={
            "title": "Standort",
            "type": "location",
        },
    )

    notes: str | None = Field(
        default=None,
        json_schema_extra={
            "title": "Notizen",
            "description": "Weitere Informationen; Freitext",
            "type": "textarea",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    assets: list[Asset] | None = Field(
        default=None,
        json_schema_extra={
            "title": "Dateien",
            "description": "Zugehörige Dateien",
            "type": "assets",
        },
    )

    assigned_users: list[UUID] = Field(
        json_schema_extra={
            "title": "Administrative Benutzer",
            "description": "Das Auto darf durch diese Benutzer vollständig administriert werden.",
            "type": "users:multi",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )


class ObjectCar(ObjectCarForm):
    id: Annotated[UUID | str, AfterValidator(lambda v: str(UUID(v)))]
    created: str
    updated: str

    @computed_field
    @property
    def name(self) -> str:
        return self.vin


class ObjectListRowCar(ObjectCar):
    vin: constr(
        to_upper=True,
        strip_whitespace=True,
        min_length=17,
        max_length=17,
        pattern=r"([A-HJ-NPR-Z0-9]{17})",
    )
    assigned_project: UUID
    assigned_users: UUID | list[UUID]
    permitted: bool = False

    @field_validator("assigned_users")
    def assigned_users_validator(cls, v):
        return list(set(ensure_list(v)))


class ObjectCarOptional(ObjectCarForm):
    vin: constr(
        to_upper=True,
        strip_whitespace=True,
        min_length=17,
        max_length=17,
        pattern=r"([A-HJ-NPR-Z0-9]{17})",
    ) | None = None
    location: Location | None = None
    vendor: str | None = None
    model: str | None = None
    year: int | None = None
    notes: str | None = None
    assets: Json[List[Asset] | Asset] | None = None
    assigned_project: UUID | None = None
    assigned_users: UUID | list[UUID] | None = None

    @field_validator("vin")
    def vin_inbound_validator(cls, v):
        if v is not None:
            if VinTool.verify_checksum(v):
                return v
            raise PydanticCustomError(
                "vin",
                "Die Checksumme der Fahrgestellnummer ist nicht korrekt",
                dict(),
            )
        return v

    @field_validator("assets")
    def assets_validator(cls, v):
        def _helper_gen(v):
            _seen = []
            for asset in ensure_list(v):
                if asset.id not in _seen:
                    _seen.append(asset.id)
                    yield asset

        if v is not None:
            return list(_helper_gen(v))

        return None

    @field_validator("assigned_users")
    def assigned_users_validator(cls, v):
        if v is not None:
            return list(set(ensure_list(v)))
        return v


class ObjectPatchCar(ObjectCarOptional):
    model_config = ConfigDict(validate_assignment=True)

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


class ObjectCarMinimal(ObjectCarOptional):
    vin: constr(
        to_upper=True,
        strip_whitespace=True,
        min_length=17,
        max_length=17,
        pattern=r"([A-HJ-NPR-Z0-9]{17})",
    )
    year: int = 0
    assigned_project: UUID
    assigned_users: UUID | list[UUID]


class ObjectAddCar(ObjectCarMinimal):
    @computed_field
    @cached_property
    def id(self) -> str:
        return str(uuid4())

    @computed_field
    @property
    def created(self) -> str:
        return utc_now_as_str()

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


class ObjectProject(ObjectProjectForm):
    id: Annotated[UUID | str, AfterValidator(lambda v: str(UUID(v)))]
    created: str
    updated: str


class ObjectListRowProject(ObjectProject):
    name: constr(min_length=1)
    location: Location | None
    assigned_users: UUID | list[UUID]
    radius: int | None = 100
    permitted: bool = False

    @field_validator("assigned_users")
    def assigned_users_validator(cls, v):
        return list(set(ensure_list(v)))


class ObjectProjectOptional(ObjectProjectForm):
    name: constr(min_length=1) | None = None
    location: Location | None = None
    notes: str | None = None
    assigned_users: UUID | list[UUID] | None = None

    @field_validator("assigned_users")
    def assigned_users_validator(cls, v):
        if v is not None:
            return list(set(ensure_list(v)))
        return v


class ObjectPatchProject(ObjectProjectOptional):
    model_config = ConfigDict(validate_assignment=True)

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


class ObjectProjectMinimal(ObjectProjectOptional):
    name: constr(min_length=1)
    assigned_users: UUID | list[UUID]


class ObjectAddProject(ObjectProjectMinimal):
    @computed_field
    @cached_property
    def id(self) -> str:
        return str(uuid4())

    @computed_field
    @property
    def created(self) -> str:
        return utc_now_as_str()

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


model_classes = {
    "types": ["cars", "projects"],
    "schemas": {
        "cars": ObjectCar.model_json_schema(),
        "projects": ObjectProject.model_json_schema(),
    },
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
    "list_row": {
        "cars": ObjectListRowCar,
        "projects": ObjectListRowProject,
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
