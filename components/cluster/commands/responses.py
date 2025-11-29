import asyncio
from .plugin import CommandPlugin
from components.logs import logger


class OkCommand(CommandPlugin):
    name = "OK"
    is_callback = True

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:  # noqa: F821
        if (
            data.ticket in cluster.callbacks
            and data.meta.name in cluster.callbacks[data.ticket]["responses"]
        ):
            callback = cluster.callbacks[data.ticket]
            callback["failed_peers"].discard(data.meta.name)
            callback["responses"][data.meta.name] = data.payload or ""
            asyncio.create_task(callback["barrier"].wait())
            logger.success(
                "▼ OK from {name} for command {cmd} ({ticket})".format(
                    name=data.meta.name,
                    cmd=callback["cmd"],
                    ticket=data.ticket,
                )
            )
        else:
            logger.warning(
                f"▼ ? OK from {data.meta.name} for unknown ticket {data.ticket}"
            )


class ErrCommand(CommandPlugin):
    name = "ERR"
    is_callback = True

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:  # noqa: F821
        if (
            data.ticket in cluster.callbacks
            and data.meta.name in cluster.callbacks[data.ticket]["responses"]
        ):
            callback = cluster.callbacks[data.ticket]
            callback["responses"][data.meta.name] = data.payload or ""
            asyncio.create_task(callback["barrier"].wait())
            logger.error(
                "▼ ERR from {name} for command {cmd} ({ticket}): {payload}".format(
                    name=data.meta.name,
                    cmd=callback["cmd"],
                    ticket=data.ticket,
                    payload=data.payload,
                )
            )
        else:
            logger.warning(
                f"▼ ? ERR from {data.meta.name} for unknown ticket {data.ticket}"
            )


class DataCommand(CommandPlugin):
    name = "DATA"
    is_callback = True

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:  # noqa: F821
        if data.ticket not in cluster.temp_data:
            cluster.temp_data[data.ticket] = {}

        if data.meta.name not in cluster.temp_data[data.ticket]:
            cluster.temp_data[data.ticket][data.meta.name] = []

        _, idx, total, partial_data = data.payload.split(" ", 3)

        cluster.temp_data[data.ticket][data.meta.name].append(partial_data)

        if idx == total:
            logger.success(f"▼ DATA from {data.meta.name} completed")
            callback = cluster.callbacks[data.ticket]
            callback["failed_peers"].discard(data.meta.name)
            callback["responses"][data.meta.name] = "".join(
                cluster.temp_data[data.ticket][data.meta.name]
            )
            asyncio.create_task(callback["barrier"].wait())
        else:
            logger.info(f"▼ DATA from {data.meta.name}, {idx}/{total}")
