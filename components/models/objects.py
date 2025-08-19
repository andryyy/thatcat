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


class ObjectProject(BaseModel):
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


class ObjectProjectOptional(ObjectProject):
    name: constr(min_length=1) | None = None
    location: Location | None = None
    notes: str | None = None
    assigned_users: UUID | list[UUID] | None = None

    @field_validator("assigned_users")
    def assigned_users_validator(cls, v):
        if v is not None:
            return list(set(ensure_list(v)))
        return v


class ObjectProjectMinimal(ObjectProjectOptional):
    name: constr(min_length=1)
    assigned_users: UUID | list[UUID]


class ObjectCar(BaseModel):
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
        default="",
        json_schema_extra={
            "title": "Hersteller",
            "description": "Hersteller des Fahrzeugs",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    model: str | None = Field(
        default="",
        json_schema_extra={
            "title": "Modell",
            "description": "Modellbezeichnung des Herstellers",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    year: int | None = Field(
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
        json_schema_extra={
            "title": "Standort",
            "type": "location",
        },
    )

    notes: str | None = Field(
        json_schema_extra={
            "title": "Notizen",
            "description": "Weitere Informationen; Freitext",
            "type": "textarea",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    assets: list[Asset] | None = Field(
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


class ObjectCarOptional(ObjectCar):
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


class ObjectBase(BaseModel):
    id: Annotated[UUID | str, AfterValidator(lambda v: str(UUID(v)))]
    created: str
    updated: str


class ObjectBaseProject(ObjectBase):
    details: ObjectProject

    @computed_field(title="name")
    @property
    def name(self) -> str:
        return self.details.name


class ObjectBaseCar(ObjectBase):
    details: ObjectCar

    @computed_field(title="vin")
    @property
    def name(self) -> str:
        return self.details.vin


class ObjectAdd(BaseModel):
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


class ObjectAddProject(ObjectAdd):
    details: ObjectProjectMinimal


class ObjectAddCar(ObjectAdd):
    details: ObjectCarMinimal


class ObjectPatch(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


class ObjectPatchProject(ObjectPatch):
    details: ObjectProjectOptional


class ObjectPatchCar(ObjectPatch):
    details: ObjectCarOptional


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
        "cars": ObjectBaseCar,
        "projects": ObjectBaseProject,
    },
    "searchables": {
        "cars": ["vin", "make", "model"],
        "projects": ["name"],
    },
    "filterables": {
        "cars": {
            "list": ["assigned_users"],
            "str": ["vin", "assigned_project"],
        },
        "projects": {
            "list": ["assigned_users"],
            "str": ["name"],
        },
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
