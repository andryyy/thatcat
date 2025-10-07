from .commands import CommandRegistry
from .commands.responses import OkCommand, ErrCommand, DataCommand
from .commands.db import SyncCommand
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
cluster.register_command(OkCommand())
cluster.register_command(ErrCommand())
cluster.register_command(DataCommand())
cluster.register_command(LockCommand())
cluster.register_command(UnlockCommand())
cluster.register_command(FileDelCommand())
cluster.register_command(FilePutCommand())
cluster.register_command(FileGetCommand())
cluster.register_command(StatusCommand())
cluster.register_command(InitCommand())
cluster.register_command(ByeCommand())
cluster.register_command(SyncCommand())
