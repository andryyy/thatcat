import asyncio
import base64
import os
import random
import zlib

from .plugin import CommandPlugin
from components.models.cluster import ErrorMessages
from components.utils import is_path_within_cwd
from components.utils.files import export_meta
from contextlib import suppress


def chunk_string(s, size=1_000_000):
    return [s[i : i + size] for i in range(0, len(s), size)]


class FileDelCommand(CommandPlugin):
    name = "FILEDEL"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        file = data.payload

        if not is_path_within_cwd(file) or not os.path.exists(file):
            return ErrorMessages.INVALID_FILE_PATH.response

        try:
            os.remove(file)
            return "OK"
        except:
            return ErrorMessages.FILE_UNLINK_FAILED.response


class FilePutCommand(CommandPlugin):
    name = "FILEPUT"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        file, dest = data.payload.split(" ")

        fileget_task = asyncio.create_task(
            cluster.files.fileget(file, dest, data.meta.name),
            name=f"fileget_{file}",
        )
        cluster.tasks.add(fileget_task)
        fileget_task.add_done_callback(cluster.tasks.discard)

        return "OK"


class FileGetCommand(CommandPlugin):
    name = "FILEGET"

    async def handle(self, cluster: "Server", data: "IncomingData") -> None:
        start, end, file = data.payload.split(" ")

        if not is_path_within_cwd(file) or not os.path.exists(file):
            return ErrorMessages.INVALID_FILE_PATH.response

        if os.stat(file).st_size < int(start):
            return ErrorMessages.START_BEHIND_FILE_END.response

        with open(file, "rb") as f:
            f.seek(int(start))
            compressed_data = zlib.compress(f.read(int(end)))
            compressed_data_encoded = base64.b64encode(compressed_data).decode("utf-8")

        chunks = chunk_string(f"{file} {export_meta(file)} {compressed_data_encoded}")
        for idx, c in enumerate(chunks, 1):
            await cluster.send_command(
                f"DATA CHUNKED {idx} {len(chunks)} {c}",
                data.meta.name,
                ticket=data.ticket,
            )
