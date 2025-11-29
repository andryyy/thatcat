from .vault import Vault
from components.models.helpers import email_validator, to_bool, to_str
from components.utils.datetimes import utc_now_as_str
from components.utils.misc import ensure_list, unique_list
from dataclasses import dataclass, field, fields, replace


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

        access_tokens = unique_list(ensure_list(self.access_tokens))
        self.access_tokens = []
        for token in access_tokens:
            if not token:
                continue
            elif isinstance(token, str) and len(token) > 15:
                self.access_tokens.append(token)
            else:
                raise ValueError(
                    "profile.access_tokens",
                    f"Token {token!r} invalid: must be a string of at least 16 characters",
                )

        self.permit_auth_requests = to_bool(self.permit_auth_requests)

        if isinstance(self.vault, dict):
            self.vault = Vault(**self.vault)

        if self.email and not email_validator(self.email):
            raise ValueError("profile.email", "'email' must be a valid email address")


@dataclass
class UserProfilePatch(UserProfileData):
    access_tokens: list[str | None] | str | None = None
    permit_auth_requests: bool | None = None
    updated: str = field(default_factory=utc_now_as_str, init=False)

    def dump_patched(self):
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) is not None
        }

    def merge(self, original: UserProfile):
        return replace(original, **self.dump_patched())
