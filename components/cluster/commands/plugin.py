import asyncio
import time

from ..exceptions import ClusterCommandFailed
from abc import ABC, abstractmethod
from components.models.cluster import CritErrors, Role
from contextlib import asynccontextmanager
from functools import wraps


def pre_dispatch(fn):
    @wraps(fn)
    async def wrapper(*args, **kwargs):
        _, cluster, data = args
        if not any(
            map(
                lambda s: data.cmd.startswith(s),
                ["ACK", "STATUS", "FULLTABLE", "INIT", "BYE"],
            )
        ):
            if not cluster.peers.local.leader:
                return CritErrors.NOT_READY.response

            if (
                cluster.peers.remotes[data.sender].cluster
                != cluster.peers.local.cluster
            ):
                return CritErrors.PEERS_MISMATCH.response

        return await fn(*args, **kwargs)

    return wrapper


class CommandPlugin(ABC):
    name: str

    @pre_dispatch
    async def dispatch(self, cluster: "Server", data: "IncomingData") -> None | str:
        async with self.timeit():
            return await self.handle(cluster, data)

    @abstractmethod
    async def handle(self, cluster: "Server", data: "IncomingData") -> None | str:
        pass

    @asynccontextmanager
    async def timeit(self):
        from components.logs import logger

        start = time.monotonic()
        try:
            yield
        except Exception as e:
            logger.critical(e)
            raise ClusterCommandFailed(e)
        finally:
            duration = time.monotonic() - start
            logger.info(f"[{self.name}] took {duration:.4f}s")


class CommandPluginLeader(CommandPlugin):
    @pre_dispatch
    async def dispatch(self, cluster: "Server", data: "IncomingData") -> None | str:
        if not cluster.peers.local.role == Role.LEADER:
            return CritErrors.UNKNOWN_COMMAND.response

        async with self.timeit():
            return await self.handle(cluster, data)
