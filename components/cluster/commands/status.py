from .plugin import CommandPlugin
from components.logs import logger


class StatusCommand(CommandPlugin):
    name = "STATUS"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        return "OK"


class InitCommand(CommandPlugin):
    name = "INIT"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        pass


class ByeCommand(CommandPlugin):
    name = "BYE"

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:
        await cluster.peers.disconnect(data.meta.name, gracefully=True)
        logger.success(f"Peer {data.meta.name} gracefully left the cluster")
