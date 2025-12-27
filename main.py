import asyncio
import signal
import ssl
import os
import sys

from pathlib import Path
from components.cluster import cluster
from components.database import db
from components.web.app import app
from config import defaults
from hypercorn.asyncio import serve
from hypercorn.config import Config
from hypercorn.middleware import ProxyFixMiddleware

_main_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
hypercorn_config = Config()
hypercorn_config.bind = [defaults.HYPERCORN_BIND]
hypercorn_config.certfile = f"{_main_dir}/{defaults.TLS_CERTFILE}"
hypercorn_config.keyfile = f"{_main_dir}/{defaults.TLS_KEYFILE}"
hypercorn_config.include_server_header = False
hypercorn_config.server_names = defaults.HOSTNAME
hypercorn_config.ciphers = "ECDHE+AESGCM"
hypercorn_config.shutdown_timeout = 0.5
hypercorn_config.graceful_timeout = 0.75
hypercorn_config.trusted_hops = 0  # n of hops to trust if proxied

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
        combined_js = Path("components/web/static_files/bundle.js")
        with open(combined_js, "w", encoding="utf-8") as combined_js_file:
            js_file_list = sorted(Path("components/web/static_files/js").rglob("*js"))
            for js_path in js_file_list:
                with open(js_path, "r", encoding="utf-8") as infile:
                    combined_js_file.write(infile.read())
                    combined_js_file.write("\n")

        async with asyncio.TaskGroup() as tg:
            db.cluster = cluster
            web_server = serve(
                ProxyFixMiddleware(
                    app, mode="legacy", trusted_hops=hypercorn_config.trusted_hops
                ),
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
