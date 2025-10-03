import asyncio
import time

from ..exceptions import CommandFailed
from abc import ABC, abstractmethod
from components.logs import logger
from components.models.cluster import ErrorMessages, Role
from contextlib import asynccontextmanager
from functools import wraps


def pre_dispatch(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        _, cluster, data = args
        if not any(
            map(
                lambda s: data.cmd.startswith(s),
                ["OK", "ERR", "STATUS", "INIT", "BYE"],
            )
        ):
            if not cluster.peers.local.leader:
                return ErrorMessages.NOT_READY.response

            if (
                cluster.peers.remotes[data.meta.name].meta.cluster
                != cluster.peers.local.cluster
            ):
                return ErrorMessages.PEERS_MISMATCH.response

        return await fn(*args, **kwargs)

    return wrapper


class CommandPlugin(ABC):
    name: str

    @pre_dispatch
    async def dispatch(self, cluster: "Server", data: "IncomingData") -> None | str:
        async with self.wrapper(data.ticket):
            return await self.handle(cluster, data)

    @abstractmethod
    async def handle(self, cluster: "Server", data: "IncomingData") -> None | str:
        pass

    @asynccontextmanager
    async def wrapper(self, ticket):
        start = time.monotonic()
        try:
            yield
        except Exception as e:
            logger.error(f"{ticket} failed")
            logger.critical(e)
            raise CommandFailed(ticket)
        finally:
            duration = time.monotonic() - start
            logger.debug(f"{self.name} ({ticket}) handled in {duration:.4f}s")


class CommandPluginLeader(CommandPlugin):
    @pre_dispatch
    async def dispatch(self, cluster: "Server", data: "IncomingData") -> None | str:
        if not cluster.peers.local.role == Role.LEADER:
            return ErrorMessages.UNKNOWN_COMMAND.response

        async with self.wrapper(data.ticket):
            return await self.handle(cluster, data)
