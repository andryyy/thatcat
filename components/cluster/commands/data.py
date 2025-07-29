from .plugin import CommandPlugin


class DataCommand(CommandPlugin):
    name = "DATA"

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:
        if not data.ticket in cluster.temp_data:
            cluster.temp_data[data.ticket] = []

        _, idx, total, partial_data = data.payload.split(" ", 3)

        cluster.temp_data[data.ticket].append(partial_data)
        if idx == total:
            cluster.callback_tickets[data.ticket].add(
                (
                    data.sender,
                    "".join(cluster.temp_data[data.ticket]),
                )
            )
