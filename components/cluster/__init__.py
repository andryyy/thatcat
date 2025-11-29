from .commands import CommandRegistry
from .commands.responses import OkCommand, ErrCommand, DataCommand
from .commands.db import SyncCommand, SyncReqCommand
from .commands.files import FileDelCommand, FileGetCommand, FilePutCommand
from .commands.locking import LockCommand, UnlockCommand
from .commands.status import ByeCommand, InitCommand, StatusCommand
from .files import Files
from .watchdog import Watchdog
from .peers import Peers
from .server import Server


cluster = Server(port=2102)
cluster.peers = Peers(cluster)
cluster.watchdog = Watchdog(cluster)
cluster.files = Files(cluster)
cluster.registry = CommandRegistry()
cluster.registry.register(OkCommand())
cluster.registry.register(ErrCommand())
cluster.registry.register(DataCommand())
cluster.registry.register(LockCommand())
cluster.registry.register(UnlockCommand())
cluster.registry.register(FileDelCommand())
cluster.registry.register(FilePutCommand())
cluster.registry.register(FileGetCommand())
cluster.registry.register(StatusCommand())
cluster.registry.register(InitCommand())
cluster.registry.register(ByeCommand())
cluster.registry.register(SyncCommand())
cluster.registry.register(SyncReqCommand())

__ALL__ = ["cluster"]
