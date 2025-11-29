from .plugin import CommandPlugin
from components.logs import logger


class StatusCommand(CommandPlugin):
    name = "STATUS"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:  # noqa: F821
        return "OK"


class InitCommand(CommandPlugin):
    name = "INIT"
    requires_callback = False

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:  # noqa: F821
        pass


class ByeCommand(CommandPlugin):
    name = "BYE"
    requires_callback = False

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:  # noqa: F821
        await cluster.peers.disconnect(data.meta.name, gracefully=True)
        logger.success(f"Peer {data.meta.name} gracefully left the cluster")
