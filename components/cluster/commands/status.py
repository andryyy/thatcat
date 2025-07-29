import asyncio

from .plugin import CommandPlugin
from components.logs import logger


class StatusCommand(CommandPlugin):
    name = "STATUS"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        return "ACK"


class InitCommand(StatusCommand):
    name = "INIT"


class ByeCommand(CommandPlugin):
    name = "BYE"

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:
        for t in cluster.tasks:
            if t.get_name() == data.sender and not cluster.stop_event.is_set():
                cluster.peers.remotes[data.sender].graceful_shutdown = True
                t.cancel()
                try:
                    await t
                    logger.success(f"Peer {data.sender} gracefully left the cluster")
                except Exception as e:
                    logger.critical(e)
                    logger.error(
                        f"Cancelling monitoring task for {data.sender} returned unexpected exception: {str(e)}"
                    )
                finally:
                    break
