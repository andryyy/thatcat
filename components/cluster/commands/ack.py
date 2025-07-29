from .plugin import CommandPlugin


class AckCommand(CommandPlugin):
    name = "ACK"

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:
        if data.ticket in cluster.callback_tickets:
            cluster.callback_tickets[data.ticket].add((data.sender, data.payload))
