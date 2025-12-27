from .credentials import Credential, CredentialAdd
from .profile import UserProfile
from components.models.helpers import to_str, to_int, validate_uuid_str, to_bool
from components.utils.datetimes import ntime_utc_now, utc_now_as_str
from components.utils.misc import ensure_list, unique_list
from dataclasses import asdict, dataclass, field, fields, replace
from typing import Protocol

USER_ACLS = ["user", "system"]


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

        acls = unique_list(ensure_list(self.acl))
        self.acl = []
        for acl in acls:
            if not acl:
                continue
            elif acl in USER_ACLS:
                self.acl.append(acl)
            else:
                raise ValueError("acl", "'acl' must contain a user ACL")

        self.login = to_str(self.login.strip())
        if len(self.login) < 3 or "@" in self.login:
            raise ValueError(
                "login",
                "'login' must be at least 3 characters long and not contain '@'",
            )

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
class UserAdd:
    id: str
    login: str
    credentials: list[CredentialAdd]
    active: bool = True
    updated: str = field(default_factory=utc_now_as_str, init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)
    doc_version: int = field(default=0, init=False)
    profile: UserProfile | dict = field(default_factory=UserProfile)

    def __post_init__(self):
        User(**asdict(self))


@dataclass
class UserPatch(UserData):
    id: str = field(default=None, init=False, repr=False)
    login: str | None = None
    credentials: list[Credential | CredentialAdd | dict | None] | None = None
    acl: list[str | None] | str | None = None
    groups: list[str | None] | str | None = None
    active: bool | None = None
    updated: str = field(default_factory=utc_now_as_str, init=False)

    def dump_patched(self):
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) is not None
        }

    def merge(self, original: Protocol):
        return replace(original, **self.dump_patched())


@dataclass
class UserSession:
    id: str
    login: str
    acl: list[str]
    cred_id: str | bytes | None = None
    lang: str = "en"
    profile: UserProfile | dict = field(default_factory=UserProfile)
    login_ts: float = field(default_factory=ntime_utc_now)


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
