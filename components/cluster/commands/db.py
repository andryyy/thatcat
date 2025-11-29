import asyncio

from ..models import ErrorMessages
from .plugin import CommandPlugin, CommandPluginLeader
from components.logs import logger


class SyncCommand(CommandPlugin):
    name = "DBSYNC"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:  # noqa: F821
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


class SyncReqCommand(CommandPluginLeader):
    name = "DBSYNCREQ"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:  # noqa: F821
        from components.database.sync import generate_full_sync_payload

        sync_payload = await generate_full_sync_payload()
        logger.info(f"Sending database dump ({len(sync_payload)} bytes)")

        return f"OK {sync_payload.decode('ascii')}"
