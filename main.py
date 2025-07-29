import asyncio
import signal
import ssl

from components.cluster import cluster
from components.logs import logger
from components.web.app import app
from config import defaults
from contextlib import asynccontextmanager
from hypercorn.asyncio import serve
from hypercorn.config import Config
from hypercorn.middleware import ProxyFixMiddleware

hypercorn_config = Config()
hypercorn_config.bind = [defaults.HYPERCORN_BIND]
hypercorn_config.certfile = defaults.TLS_CERTFILE
hypercorn_config.keyfile = defaults.TLS_KEYFILE
hypercorn_config.include_server_header = False
hypercorn_config.server_names = defaults.HOSTNAME
hypercorn_config.ciphers = "ECDHE+AESGCM"
hypercorn_config.shutdown_timeout = 1.25

app.stop_event = asyncio.Event()
cluster_stop_event = asyncio.Event()


class TerminateTaskGroup(Exception):
    """Exception raised to terminate a task group."""


def handle_shutdown() -> None:
    cluster_stop_event.set()


async def main():
    def _exception_handler(loop, context):
        exception = context.get("exception")
        if isinstance(exception, ssl.SSLError):
            pass
        else:
            loop.default_exception_handler(context)

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(_exception_handler)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_shutdown)

    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                serve(
                    ProxyFixMiddleware(app, mode="legacy", trusted_hops=1),
                    hypercorn_config,
                    shutdown_trigger=app.stop_event.wait,
                ),
                name="qrt",
            )
            tg.create_task(
                cluster.run(
                    stop_event=cluster_stop_event, post_stop_event=app.stop_event
                ),
                name="cluster",
            )
            await app.stop_event.wait()
            logger.info("Waiting for graceful shutdown")
            await asyncio.sleep(hypercorn_config.shutdown_timeout + 0.5)
            raise TerminateTaskGroup()

    except* Exception as e:
        if not app.stop_event.is_set():
            for exc in e.exceptions:
                logger.critical(exc)
            raise
        pass


asyncio.run(main())
