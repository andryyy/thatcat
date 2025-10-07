import json
import random

from components.models.helpers import *
from components.utils.datetimes import ntime_utc_now, utc_now_as_str
from components.utils.misc import ensure_list, unique_list
from config.defaults import ACCEPT_LANGUAGES
from dataclasses import asdict, dataclass, field, fields
from functools import cached_property

USER_ACLS = ["user", "system"]


@dataclass
class PatchTemplate:
    def dump_patched(self):
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) is not None
        }


@dataclass
class UsersPagination:
    page: int | str
    page_size: int | str
    sort_attr: str
    sort_reverse: bool | str
    pages: int | str = 0
    elements: int | str = 0

    def __post_init__(self) -> None:
        for name in ("page_size", "pages", "elements"):
            setattr(self, name, to_int(getattr(self, name)))

        self.page = to_int(self.page) or 1
        self.sort_reverse = to_bool(self.sort_reverse)

        if not isinstance(self.sort_attr, str):
            raise TypeError(
                "sort_attr",
                f"'sort_attr' must be string, got {type(self.sort_attr).__name__}",
            )


@dataclass
class Vault:
    public_key_pem: str
    wrapped_private_key: str
    iv: str
    salt: str

    def __post_init__(self) -> None:
        for f in fields(self):
            setattr(self, f, to_str(getattr(self, f)))


@dataclass
class TokenConfirmation:
    confirmation_code: int | str
    token: str

    def __post_init__(self) -> None:
        self.confirmation_code = "%06d" % to_int(self.confirmation_code)
        self.token = to_str(self.token.strip())
        if len(self.token) != 14:
            raise ValueError("token", "'token' has wrong length")


@dataclass
class Authentication:
    login: str
    id: str | None = None

    @cached_property
    def token(self) -> str:
        return "%04d-%04d-%04d" % (
            random.randint(0, 9999),
            random.randint(0, 9999),
            random.randint(0, 9999),
        )

    def __post_init__(self) -> None:
        self.login = to_str(self.login.strip())
        if len(self.login) < 3:
            raise ValueError("login", "'login' must be at least 3 characters long")

        if self.id is not None:
            self.id = validate_uuid_str(self.id)


@dataclass
class UserProfileData:
    updated: str = field(default_factory=utc_now_as_str)
    vault: Vault | dict | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    access_tokens: list[str | None] | str = field(default_factory=list)
    permit_auth_requests: bool = True


@dataclass
class UserProfile(UserProfileData):
    def __post_init__(self) -> None:
        for name in ("first_name", "last_name", "email", "updated"):
            if getattr(self, name):
                setattr(self, name, to_str(getattr(self, name).strip()) or None)

        self.access_tokens = unique_list(ensure_list(self.access_tokens))
        if not all(
            isinstance(item, str) and len(item) > 15 for item in self.access_tokens
        ):
            raise ValueError(
                "access_tokens",
                "Tokens in 'access_tokens' must have at least 16 characters",
            )

        self.permit_auth_requests = to_bool(self.permit_auth_requests)

        if not self.vault:
            self.vault = None
        elif isinstance(self.vault, dict):
            self.vault = Vault(**self.vault)

        if self.email and not email_validator(self.email):
            raise ValueError("email", "'email' must be a valid email address")


@dataclass
class CredentialBase:
    updated: str
    created: str


@dataclass
class CredentialData:
    id: str | bytes
    public_key: str
    friendly_name: str = "New passkey"
    sign_count: int = 0
    active: bool = True
    last_login: str | None = None


@dataclass
class Credential(CredentialData, CredentialBase):
    def __post_init__(self):
        if not isinstance(self.id, (str, bytes)) or self.id == "":
            raise ValueError("id", "'id' must be a non-empty string or bytes")

        if isinstance(self.id, bytes):
            self.id = self.id.hex()

        if (
            not isinstance(self.public_key, str)
            or to_str(self.public_key.strip()) == ""
        ):
            raise ValueError("public_key", "'public_key' must be a non-empty string")

        if not isinstance(self.updated, str) or to_str(self.updated.strip()) == "":
            raise ValueError("updated", "'updated' must be a non-empty string")

        if not isinstance(self.created, str) or to_str(self.created.strip()) == "":
            raise ValueError("created", "'created' must be a non-empty string")

        if self.last_login is not None:
            self.last_login = to_str(self.last_login.strip()) or None

        self.sign_count = to_int(self.sign_count)
        self.active = to_bool(self.active)

        self.friendly_name = to_str(self.friendly_name.strip())
        if not self.friendly_name:
            self.friendly_name = "New passkey"


@dataclass
class CredentialPatch(CredentialData, PatchTemplate):
    updated: str = field(default_factory=utc_now_as_str, init=False)
    id: str = field(default=None, init=False, repr=False)
    active: bool | None = None
    sign_count: int | None = None
    public_key: str | None = None
    friendly_name: str | None = None


@dataclass
class CredentialAdd(CredentialData):
    updated: str = field(default_factory=utc_now_as_str, init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)

    def __post_init__(self):
        Credential(**asdict(self))
        if isinstance(self.id, bytes):
            self.id = self.id.hex()


@dataclass
class UserBase:
    id: str
    updated: str
    created: str
    doc_version: int | str


@dataclass
class UserData:
    login: str
    credentials: list[Credential | CredentialAdd | dict | None] = field(
        default_factory=list
    )
    acl: list[str | None] | str = field(default_factory=list)
    groups: list[str | None] | str = field(default_factory=list)
    profile: UserProfile | dict = field(default_factory=UserProfile)
    active: bool = True


@dataclass
class User(UserData, UserBase):
    def __post_init__(self) -> None:
        self.id = validate_uuid_str(self.id)
        self.doc_version = to_int(self.doc_version)

        if not isinstance(self.updated, str) or self.updated == "":
            raise ValueError("updated", "'updated' must be a non-empty string")

        if not isinstance(self.created, str) or self.created == "":
            raise ValueError("created", "'created' must be a non-empty string")

        self.groups = unique_list(ensure_list(self.groups))
        if not all(isinstance(item, str) and len(item) > 0 for item in self.groups):
            raise ValueError("groups", "'groups' must contain non-empty strings")

        self.acl = unique_list(ensure_list(self.acl))
        if not all(acl in USER_ACLS for acl in self.acl):
            raise ValueError("acl", "'acl' must contain a user ACL")

        self.login = to_str(self.login.strip())
        if len(self.login) < 3:
            raise ValueError("login", "'login' must be at least 3 characters long")

        self.active = to_bool(self.active)

        if self.credentials == "":
            self.credentials = []
        elif self.credentials != []:
            credentials = []
            for credential in self.credentials:
                if credential not in credentials:
                    if isinstance(credential, dict):
                        credentials.append(Credential(**credential))
                    elif isinstance(credential, (Credential, CredentialAdd)):
                        credentials.append(credential)
                    else:
                        raise TypeError(
                            "credentials",
                            f"Invalid type for 'credentials': {type(credential).__name__}",
                        )
            self.credentials = credentials

        if isinstance(self.profile, dict):
            self.profile = UserProfile(**self.profile)
        elif not isinstance(self.profile, UserProfile):
            raise TypeError(
                "profile", f"Invalid type for 'profile': {type(self.profile).__name__}"
            )


@dataclass
class UserGroups:
    name: str
    new_name: str
    members: list[str] | str

    def __post_init__(self) -> None:
        self.name = to_str(self.name.strip())
        if len(self.name) < 1:
            raise ValueError("name", "'name' must be at least 1 character long")

        self.new_name = to_str(self.new_name.strip())
        if len(self.new_name) < 1:
            raise ValueError("new_name", "'new_name' must be at least 1 character long")

        self.members = [
            validate_uuid_str(u) for u in unique_list(ensure_list(self.members))
        ]
        if not self.members:
            raise ValueError("members", "'members' must not be empty")


@dataclass
class UserAdd(UserData):
    updated: str = field(default_factory=utc_now_as_str, init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)

    def __post_init__(self):
        User(**asdict(self))


@dataclass
class UserProfilePatch(UserProfileData, PatchTemplate):
    access_tokens: list[str | None] | str | None = None
    permit_auth_requests: bool | None = None
    updated: str = field(default_factory=utc_now_as_str, init=False)


@dataclass
class UserPatch(UserData, PatchTemplate):
    id: str = field(default=None, init=False, repr=False)
    profile: UserProfilePatch | None = None
    login: str | None = None
    credentials: list[Credential | CredentialAdd | dict | None] | None = None
    acl: list[str | None] | str | None = None
    groups: list[str | None] | str | None = None
    active: bool | None = None
    updated: str = field(default_factory=utc_now_as_str, init=False)


@dataclass
class UserSession:
    id: str
    login: str
    acl: list[str]
    cred_id: str | bytes | None = None
    lang: str = "en"
    profile: UserProfile | dict = field(default_factory=UserProfile)
    login_ts: float = field(default_factory=ntime_utc_now)
