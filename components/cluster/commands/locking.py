import asyncio
import random

from .plugin import CommandPluginLeader
from components.logs import logger
from components.models.cluster import CritErrors
from contextlib import suppress


class LockCommand(CommandPluginLeader):
    name = "LOCK"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        try:
            lock_id, lock_tables = data.payload.split(" ")
            tables = lock_tables.split(",")
            locked_tables = set()
            for t in tables:
                if t not in cluster.locks:
                    cluster.locks[t] = {
                        "lock": asyncio.Lock(),
                        "id": None,
                    }
                await asyncio.wait_for(
                    cluster.locks[t]["lock"].acquire(),
                    0.05 + random.uniform(0.05, 0.1),
                )
                locked_tables.add(t)
                cluster.locks[t]["id"] = lock_id
        except TimeoutError:
            cluster._release_tables(lock_id, locked_tables)
            return "ACK BUSY"
        except Exception as e:
            logger.critical(f"Unhandled LOCK error: {str(e)}")
            cluster._release_tables(lock_id, locked_tables)
            return CritErrors.LOCK_ERROR.response
        else:
            return "ACK"


class UnlockCommand(CommandPluginLeader):
    name = "UNLOCK"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        lock_id, lock_tables = data.payload.split(" ")
        tables = lock_tables.split(",")

        for t in tables:
            if lock_id != cluster.locks[t]["id"]:
                return CritErrors.UNLOCK_ERROR_UNKNOWN_ID.response

        cluster._release_tables(lock_id, tables)
        return "ACK"
