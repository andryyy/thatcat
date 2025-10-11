import asyncio
import signal
import ssl

from components.cluster import cluster
from components.database import db
from components.web.app import app
from config import defaults
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
hypercorn_config.shutdown_timeout = 0.5
hypercorn_config.graceful_timeout = 0.75

app.stop_event = asyncio.Event()


def handle_shutdown() -> None:
    app.stop_event.set()


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
            db.cluster = cluster
            web_server = serve(
                ProxyFixMiddleware(app, mode="legacy", trusted_hops=1),
                hypercorn_config,
                shutdown_trigger=app.stop_event.wait,
            )
            tg.create_task(
                web_server,
                name="qrt",
            )
            tg.create_task(
                cluster.run(shutdown_trigger=app.stop_event),
                name="cluster",
            )
            await app.stop_event.wait()
    except* ssl.SSLError as e:
        for err in e.exceptions:
            if (
                app.stop_event.is_set()
                and err.reason == "APPLICATION_DATA_AFTER_CLOSE_NOTIFY"
            ):
                pass


asyncio.run(main())
