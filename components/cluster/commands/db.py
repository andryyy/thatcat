import asyncio

from .plugin import CommandPlugin
from components.logs import logger
from components.models.cluster import ErrorMessages


class SyncCommand(CommandPlugin):
    name = "DBSYNC"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        async def _dbsync(payload):
            from components.database import db

            async with db:
                await db.sync_in(payload)

        strategy, payload = data.payload.split(" ")

        try:
            if strategy == "LAZY":
                asyncio.create_task(_dbsync(payload))
            elif strategy == "BLOCK":
                await _dbsync(payload)
            else:
                raise ValueError("Unknown strategy")
            return "OK"
        except Exception as e:
            logger.critical(e)
            return ErrorMessages.SYNC_ERROR.response
