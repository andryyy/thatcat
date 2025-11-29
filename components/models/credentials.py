from components.models.helpers import to_str, to_int, to_bool
from dataclasses import asdict, dataclass, field, fields, replace
from components.utils.datetimes import utc_now_as_str


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
class CredentialPatch(CredentialData):
    updated: str = field(default_factory=utc_now_as_str, init=False)
    id: str = field(default=None, init=False, repr=False)
    active: bool | None = None
    sign_count: int | None = None
    public_key: str | None = None
    friendly_name: str | None = None

    def dump_patched(self):
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) is not None
        }

    def merge(self, original: Credential):
        return replace(original, **self.dump_patched())


@dataclass
class CredentialAdd(CredentialData):
    updated: str = field(default_factory=utc_now_as_str, init=False)
    created: str = field(default_factory=utc_now_as_str, init=False)

    def __post_init__(self):
        Credential(**asdict(self))
        if isinstance(self.id, bytes):
            self.id = self.id.hex()
