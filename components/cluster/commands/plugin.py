import time

from ..exceptions import CommandFailed
from abc import ABC, abstractmethod
from components.logs import logger
from ..models import ErrorMessages, Role
from contextlib import asynccontextmanager


class CommandPlugin(ABC):
    name: str
    is_callback: bool = False
    requires_callback: bool = True

    async def dispatch(self, cluster: "Server", data: "IncomingData") -> None | str:  # noqa: F821
        if (
            data.cmd not in {"OK", "ERR", "STATUS", "INIT", "BYE"}
            and not cluster.peers.peers_consistent()
        ):
            return ErrorMessages.NOT_READY.response

        async with self.wrapper(data.ticket):
            return await self.handle(cluster, data)

    @abstractmethod
    async def handle(self, cluster: "Server", data: "IncomingData") -> None | str:  # noqa: F821
        pass

    @asynccontextmanager
    async def wrapper(self, ticket):
        start = time.monotonic()
        try:
            yield
        except Exception as e:
            logger.error(f"{ticket} failed")
            logger.critical(e, exc_info=True)
            raise CommandFailed(ticket)
        finally:
            duration = time.monotonic() - start
            logger.debug(f"{self.name} ({ticket}) handled in {duration:.4f}s")


class CommandPluginLeader(CommandPlugin):
    async def dispatch(self, cluster: "Server", data: "IncomingData") -> None | str:  # noqa: F821
        if (
            data.cmd not in {"OK", "ERR", "STATUS", "INIT", "BYE"}
            and not cluster.peers.peers_consistent()
        ):
            return ErrorMessages.NOT_READY.response

        if not cluster.peers.local.role == Role.LEADER:
            return ErrorMessages.UNKNOWN_COMMAND.response

        async with self.wrapper(data.ticket):
            return await self.handle(cluster, data)
