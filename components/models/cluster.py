import asyncio

from components.cluster.ssl import get_ssl_context
from components.models import *
from components.utils import ensure_unique_list
from components.utils.datetimes import ntime_utc_now
from config import defaults
from contextlib import closing


class Role(Enum):
    LEADER = 1
    FOLLOWER = 2


class ConnectionStatus(Enum):
    CONNECTED = 0
    REFUSED = 1
    SOCKET_REFUSED = 2
    ALL_AVAILABLE_FAILED = 3
    OK = 4
    OK_WITH_PREVIOUS_ERRORS = 5


class CritErrors(Enum):
    CANNOT_APPLY = "CRIT:CANNOT_APPLY"
    CANNOT_COMMIT = "CRIT:CANNOT_COMMIT"
    FILE_UNLINK_FAILED = "CRIT:FILE_UNLINK_FAILED"
    INVALID_FILE_PATH = "CRIT:INVALID_FILE_PATH"
    LOCK_ERROR = "CRIT:LOCK_ERROR"
    NOT_READY = "CRIT:NOT_READY"
    NOTHING_TO_COMMIT = "CRIT:NOTHING_TO_COMMIT"
    PATCH_EXCEPTION = "CRIT:PATCH_EXCEPTION"
    PEERS_MISMATCH = "CRIT:PEERS_MISMATCH"
    START_BEHIND_FILE_END = "CRIT:START_BEHIND_FILE_END"
    TABLE_HASH_MISMATCH = "CRIT:TABLE_HASH_MISMATCH"
    UNKNOWN_COMMAND = "CRIT:UNKNOWN_COMMAND"
    COMMAND_FAILED = "CRIT:COMMAND_FAILED"
    UNLOCK_ERROR_UNKNOWN_ID = "CRIT:UNLOCK_ERROR_UNKNOWN_ID"
    ZOMBIE = "CRIT:ZOMBIE"

    @property
    def response(self):
        return f"ACK {self.value}"


class SendCommandReturn(BaseModel):
    ticket: Annotated[str | float, AfterValidator(lambda v: str(v))]
    receivers: Annotated[list, AfterValidator(lambda v: ensure_unique_list(v))]


class IncomingData(BaseModel):
    ticket: str
    cmd: str
    sender: str

    @computed_field
    @property
    def payload(self) -> str:
        _, _, payload = self.cmd.partition(" ")
        return payload


class LocalPeer(BaseModel):
    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    @model_validator(mode="before")
    @classmethod
    def pre_init(cls, data: Any) -> Any:
        if not data["ip4"] and not data["ip6"]:
            raise ValueError("Neither a IPv4 nor a IPv6 address was provided")
        return data

    name: constr(pattern=r"^[a-zA-Z0-9\-_\.]+$", min_length=3)
    ip4: IPv4Address | None = None
    ip6: IPv6Address | None = None
    cli_bindings: list[IPvAnyAddress] = defaults.CLUSTER_CLI_BINDINGS
    leader: str | None = None
    role: Role = Role.FOLLOWER
    cluster: str = ""
    started: float = ntime_utc_now()
    cluster_complete: bool = False

    @computed_field
    @property
    def _bindings_as_str(self) -> str:
        return [str(ip) for key in ("ip4", "ip6") if (ip := getattr(self, key))]

    @computed_field
    @property
    def _all_bindings_as_str(self) -> str:
        return [
            str(ip) for key in ("ip4", "ip6") if (ip := getattr(self, key))
        ] + self.cli_bindings

    @model_validator(mode="after")
    def cli_bindings_validator(self):
        for ip in self.cli_bindings:
            if ip == self.ip4 or ip == self.ip6:
                raise ValueError("CLI bindings overlap local bindings")
        return self


class Streams(BaseModel):
    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    egress: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None = None
    ingress: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None = None


class RemotePeer(BaseModel):
    model_config = ConfigDict(validate_assignment=True, arbitrary_types_allowed=True)

    @model_validator(mode="before")
    @classmethod
    def pre_init(cls, data: Any) -> Any:
        if not data["ip4"] and not data["ip6"]:
            raise ValueError("Neither a IPv4 nor a IPv6 address was provided")
        return data

    lock: asyncio.Lock = asyncio.Lock()
    cluster: str = ""
    leader: str | None = None
    started: float | None = None
    name: constr(pattern=r"^[a-zA-Z0-9\-_\.]+$", min_length=3)
    ip4: IPv4Address | None = None
    ip6: IPv6Address | None = None
    nat_ip4: IPvAnyAddress | None = None
    graceful_shutdown: bool = False
    streams: Streams = Streams()
    port: int = 2102

    @computed_field
    @property
    def ips(self) -> list:
        return [
            str(ip) for key in ("ip4", "ip6", "nat_ip4") if (ip := getattr(self, key))
        ]

    @computed_field
    @property
    def healthy(self) -> str:
        if (
            self.streams.egress
            and self.streams.ingress
            and self.cluster
            and self.started
            and self.leader
        ):
            return True
        return False
