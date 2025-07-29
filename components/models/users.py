import random
import json
from config.defaults import ACCEPT_LANGUAGES
from components.utils import ensure_list, to_unique_sorted_str_list
from components.utils.datetimes import ntime_utc_now, utc_now_as_str
from webauthn.helpers.structs import AuthenticatorTransport
from components.models import *
from components.models.objects import model_classes

USER_FILTERABLES = ["list:acl", "list:groups"]
USER_ACLS = ["user", "system"]


class UsersPagination(BaseModel):
    page: int
    page_size: int
    sort_attr: str
    sort_reverse: bool
    pages: int = 0
    elements: int = 0


class Tresor(BaseModel):
    public_key_pem: str
    wrapped_private_key: str
    iv: str
    salt: str


class TokenConfirmation(BaseModel):
    confirmation_code: Annotated[int, AfterValidator(lambda i: "%06d" % i)]
    token: str = constr(strip_whitespace=True, min_length=14, max_length=14)


class AuthToken(BaseModel):
    login: str = constr(strip_whitespace=True, min_length=1)

    @computed_field
    @cached_property
    def token(self) -> str:
        return "%04d-%04d-%04d" % (
            random.randint(0, 9999),
            random.randint(0, 9999),
            random.randint(0, 9999),
        )


class UserProfile(BaseModel):
    @computed_field
    @property
    def _form_id(self) -> str:
        return f"form-{str(uuid4())}"

    model_config = ConfigDict(validate_assignment=True)

    tresor: Tresor | dict = Field(
        default={},
        json_schema_extra={
            "title": "🔐 Einen Tresor konfigurieren",
            "type": "tresor",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    first_name: str | None = Field(
        default="",
        json_schema_extra={
            "title": "Vorname",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    last_name: str | None = Field(
        default="",
        json_schema_extra={
            "title": "Nachname",
            "type": "text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    email: str | None = Field(
        default="",
        json_schema_extra={
            "title": "E-Mail Adresse",
            "description": "Eine E-Mail Adresse ist optional",
            "type": "email",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    access_tokens: list[constr(min_length=16) | None] | None = Field(
        default=None,
        json_schema_extra={
            "title": "API Keys",
            "description": "API Keys können zum maschinellen Zugriff auf das System verwendet werden",
            "type": "list:text",
            "input_extra": 'autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"',
        },
    )

    permit_auth_requests: bool | None = Field(
        default=True,
        json_schema_extra={
            "title": "Login-Anfragen erlauben",
            "description": "Andere Geräte können Login-Anfragen für einen schnellen Zugang stellen.",
            "type": "toggle",
            "input_extra": 'autocomplete="off"',
        },
    )

    updated: str | None = None


class Credential(BaseModel):
    id: Annotated[str, AfterValidator(lambda x: bytes.fromhex(x))] | bytes
    public_key: Annotated[str, AfterValidator(lambda x: bytes.fromhex(x))] | bytes
    friendly_name: constr(strip_whitespace=True, min_length=1)
    last_login: str
    sign_count: int
    transports: list[AuthenticatorTransport] | None = []
    active: bool
    updated: str
    created: str

    @field_serializer("id", "public_key")
    def serialize_bytes_to_hex(self, v: bytes, _info):
        return v.hex() if isinstance(v, bytes) else v


class CredentialAdd(BaseModel):
    id: Annotated[str, AfterValidator(lambda x: bytes.fromhex(x))] | bytes
    public_key: Annotated[str, AfterValidator(lambda x: bytes.fromhex(x))] | bytes
    sign_count: int
    friendly_name: constr(strip_whitespace=True, min_length=1) = "New passkey"
    transports: list[AuthenticatorTransport] | None = []
    active: bool = True
    last_login: str = ""

    @computed_field
    @property
    def created(self) -> str:
        return utc_now_as_str()

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()

    @field_serializer("id", "public_key")
    def serialize_bytes_to_hex(self, v: bytes, _info):
        return v.hex() if isinstance(v, bytes) else v


class User(BaseModel):
    model_config = ConfigDict(validate_assignment=True)

    id: Annotated[str, AfterValidator(lambda v: str(UUID(v)))]
    login: constr(strip_whitespace=True, min_length=1)
    credentials: list[Credential | CredentialAdd] = []
    acl: list
    groups: Annotated[
        constr(strip_whitespace=True, min_length=1)
        | list[constr(strip_whitespace=True, min_length=1)],
        AfterValidator(lambda v: ensure_list(v)),
    ] = []
    profile: UserProfile
    created: str
    updated: str


class UserGroups(BaseModel):
    name: constr(strip_whitespace=True, min_length=1)
    new_name: constr(strip_whitespace=True, min_length=1)
    members: Annotated[
        str | list,
        AfterValidator(lambda x: to_unique_sorted_str_list(ensure_list(x))),
    ] = []


class UserAdd(BaseModel):
    login: str = constr(strip_whitespace=True, min_length=1)
    credentials: list[str] = []
    acl: Annotated[
        Literal[*USER_ACLS] | list[Literal[*USER_ACLS]],
        AfterValidator(lambda v: ensure_list(v)),
    ] = ["user"]
    profile: UserProfile = UserProfile.model_validate({})
    groups: list[constr(strip_whitespace=True, min_length=1)] = []

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


class UserPatch(BaseModel):
    login: str | None = None
    acl: Annotated[
        Literal[*USER_ACLS] | list[Literal[*USER_ACLS]],
        AfterValidator(lambda v: ensure_list(v)),
    ] = []
    groups: str | list | None = None

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


class UserProfilePatch(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    tresor: Json[Tresor] | Literal[""] | None = None
    access_tokens: constr(min_length=16) | list[constr(min_length=16)] | None = None
    permit_auth_requests: bool | None = None

    @field_validator("email", mode="before")
    def email_validator(cls, v):
        if v in [None, ""]:
            return ""
        try:
            email = validate_email(v, check_deliverability=False).ascii_email
        except:
            raise PydanticCustomError(
                "email",
                "Die E-Mail Adresse ist ungültig",
                dict(),
            )
        return email

    @field_validator("tresor")
    def tresor_validator(cls, v):
        if v is not None:
            if v == "":
                return {}
        return v

    @field_validator("access_tokens")
    def access_tokens_validator(cls, v):
        if v is not None:
            return list(set(ensure_list(v)))
        return v

    @field_validator("permit_auth_requests", mode="before")
    def permit_auth_requests_validator(cls, v):
        return v

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


class CredentialPatch(BaseModel):
    friendly_name: constr(strip_whitespace=True, min_length=1) | None = None
    active: bool | None = None
    last_login: str | None = None
    sign_count: int | None = None

    @computed_field
    @property
    def updated(self) -> str:
        return utc_now_as_str()


class UserSession(BaseModel):
    id: str
    login: str
    acl: list | str
    cred_id: str | None = None
    lang: Literal[*ACCEPT_LANGUAGES] = "en"
    profile: dict | UserProfile | None = {}
    login_ts: float = Field(default_factory=ntime_utc_now)
    callbacks: list = []
