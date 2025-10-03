import asyncio
import random

from .plugin import CommandPluginLeader
from components.logs import logger
from components.models.cluster import ErrorMessages
from contextlib import suppress


class LockCommand(CommandPluginLeader):
    name = "LOCK"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        try:
            lock_id, lock_objects = data.payload.split(" ")
            lock_objects = lock_objects.split(",")
            locked_objects = set()
            for l in lock_objects:
                if l not in cluster.locks:
                    cluster.locks[l] = {
                        "lock": asyncio.Lock(),
                        "id": None,
                    }
                await asyncio.wait_for(
                    cluster.locks[l]["lock"].acquire(),
                    0.05 + random.uniform(0.05, 0.1),
                )
                locked_objects.add(l)
                cluster.locks[l]["id"] = lock_id
        except TimeoutError:
            cluster._release_locks(lock_id, locked_objects)
            return "OK BUSY"
        except Exception as e:
            logger.critical(f"Unhandled LOCK error: {str(e)}")
            cluster._release_locks(lock_id, locked_objects)
            return ErrorMessages.LOCK_ERROR.response
        else:
            return "OK"


class UnlockCommand(CommandPluginLeader):
    name = "UNLOCK"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        lock_id, lock_objects = data.payload.split(" ")
        lock_objects = lock_objects.split(",")

        for l in lock_objects:
            if lock_id != cluster.locks[l]["id"]:
                return ErrorMessages.UNLOCK_ERROR_UNKNOWN_ID.response

        cluster._release_locks(lock_id, lock_objects)
        return "OK"
