import asyncio
import re

from components.cluster.ssl import get_ssl_context
from components.utils import ntime_utc_now, unique_list, ensure_list
from config import defaults
from contextlib import closing
from dataclasses import dataclass, field
from enum import Enum


READER_DATA_PATTERN = re.compile(
    r"(?P<ticket>\S+)\s+"
    r"(?P<cmd>\S+)\s*"
    r"(?P<payload>.*?)\s*"
    r":META\s+"
    r"NAME\s(?P<name>\S+)\s+"
    r"CLUSTER\s(?P<cluster>\S+)\s+"
    r"STARTED\s(?P<started>\S+)\s+"
    r"LEADER\s(?P<leader>\S+)"
)


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


class ErrorMessages(Enum):
    FILE_UNLINK_FAILED = "FILE_UNLINK_FAILED"
    INVALID_FILE_PATH = "INVALID_FILE_PATH"
    LOCK_ERROR = "LOCK_ERROR"
    NOT_READY = "NOT_READY"
    SYNC_ERROR = "SYNC_ERROR"
    PEERS_MISMATCH = "PEERS_MISMATCH"
    START_BEHIND_FILE_END = "START_BEHIND_FILE_END"
    UNKNOWN_COMMAND = "UNKNOWN_COMMAND"
    COMMAND_FAILED = "COMMAND_FAILED"
    UNLOCK_ERROR_UNKNOWN_ID = "UNLOCK_ERROR_UNKNOWN_ID"

    @property
    def response(self):
        return f"ERR {self.value}"


@dataclass
class MetaData:
    cluster: str | None = None
    leader: str | None = None
    started: str | int | float | None = None
    name: str | None = None

    def __bool__(self) -> bool:
        for k in self.__dict__.keys():
            if getattr(self, k) is not None:
                return True
        return False

    def __post_init__(self):
        if self.name is not None and self.name not in [
            _["name"] for _ in defaults.CLUSTER_PEERS
        ]:
            raise ValueError("Invalid peer name")
        if self.leader == "?CONFUSED":
            self.leader = None
        if self.cluster == "?CONFUSED":
            self.cluster = None
        if self.started is not None:
            self.started = float(self.started)


@dataclass
class IncomingData:
    ticket: str
    cmd: str
    payload: str
    meta: MetaData


@dataclass
class LocalPeer:
    name: str
    ip4: str | None = None
    ip6: str | None = None
    cli_bindings: list = field(default_factory=lambda: defaults.CLUSTER_CLI_BINDINGS)
    leader: str | None = None
    role: Role = Role.FOLLOWER
    cluster: str = ""
    started: float = field(default_factory=ntime_utc_now)
    cluster_complete: asyncio.Event = field(default_factory=asyncio.Event)

    def __post_init__(self):
        if not self.ip4 and not self.ip6:
            raise ValueError("Neither an IPv4 nor an IPv6 address was provided")

        if not re.fullmatch(r"^[a-zA-Z0-9\-_\.]+$", self.name) or len(self.name) < 3:
            raise ValueError(f"'{self.name}' is not a valid name")

        self.cli_bindings = unique_list(ensure_list(self.cli_bindings))

        for ip in self.cli_bindings:
            if ip == self.ip4 or ip == self.ip6:
                raise ValueError(
                    "CLI bindings cannot overlap with the peer's own IP address"
                )

    @property
    def server_bindings(self) -> list[str]:
        bindings = [str(ip) for key in ("ip4", "ip6") if (ip := getattr(self, key))]
        bindings.extend([str(ip) for ip in self.cli_bindings])
        return bindings


@dataclass
class Streams:
    egress: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None = None
    ingress: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None = None


@dataclass
class RemotePeer:
    name: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    streams: Streams = field(default_factory=Streams)
    meta: MetaData = field(default_factory=MetaData)
    ip4: str | None = None
    ip6: str | None = None
    nat_ip4: str | str | None = None
    graceful_shutdown: bool = False
    port: int = 2102

    def __post_init__(self):
        if not self.ip4 and not self.ip6:
            raise ValueError("A peer must have at least an IPv4 or IPv6 address")

    @property
    def ips(self) -> list[str]:
        return [
            str(ip) for key in ("ip4", "ip6", "nat_ip4") if (ip := getattr(self, key))
        ]

    @property
    def established(self) -> bool:
        return bool(self.streams.egress and self.streams.ingress and self.meta)
