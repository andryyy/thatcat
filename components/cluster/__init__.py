from .files import Files
from .monitor import Monitor
from .peers import Peers
from .server import Server
from .commands import CommandRegistry
from .commands.ack import AckCommand
from .commands.data import DataCommand
from .commands.locking import LockCommand, UnlockCommand
from .commands.files import FileDelCommand, FilePutCommand, FileGetCommand
from .commands.status import StatusCommand, InitCommand, ByeCommand
from .commands.db import SyncCommand

cluster = Server(port=2102)
cluster.peers = Peers()
cluster.monitor = Monitor(cluster)
cluster.files = Files(cluster)
cluster.registry = CommandRegistry()
cluster.register_command(AckCommand())
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
