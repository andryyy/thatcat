import asyncio
import json
import base64

from ..exceptions import *
from .plugin import CommandPlugin
from components.logs import logger
from components.models.cluster import CritErrors
from components.utils.cryptography import dict_digest_sha1


class SyncCommand(CommandPlugin):
    name = "SYNC"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        from components.database import db

        data.payload.split(" ")

        try:
            async with db:
                await db.sync_in(data.payload)
            return "ACK"
        except:
            return CritErrors.SYNC_ERROR.response
