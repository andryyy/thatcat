import random

from .plugin import CommandPluginLeader
from components.logs import logger
from ..models import ErrorMessages


class LockCommand(CommandPluginLeader):
    name = "LOCK"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        try:
            lock_id, lock_objects = data.payload.split(" ")
            lock_objects = lock_objects.split(",")
            timeout = 0.05 + random.uniform(0.05, 0.1)
            await cluster._acquire_leader_locks(lock_id, lock_objects, timeout)
        except TimeoutError:
            return "OK BUSY"
        except Exception as e:
            logger.critical(f"Unhandled LOCK error: {str(e)}", exc_info=True)
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
