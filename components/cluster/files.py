import asyncio
import base64
import os
import zlib

from .exceptions import *
from components.logs import logger
from components.utils import is_path_within_cwd, apply_meta


class Files:
    def __init__(self, cluster: "Server"):
        self.cluster = cluster

    async def filedel(self, file: str, peer: str):
        if not isinstance(file, str):
            raise ValueError(f"'file' must be string, got {type(file).__name__}")

        if not isinstance(peer, str):
            raise ValueError(f"'peer' must be string, got {type(peer).__name__}")

        try:
            if not is_path_within_cwd(file):
                raise ValueError("File not within working directory")

            if peer not in self.cluster.peers.get_established():
                raise OfflinePeer(peer)

            await self.cluster.send_command(f"FILEDEL {file}", peer)

        except Exception as e:
            raise FileDelException(e)

    async def fileget(
        self,
        file: str,
        dest: str,
        peer: str,
        startb: int = 0,
        endb: int = -1,
    ):
        if not isinstance(file, str):
            raise ValueError(f"'file' must be string, got {type(file).__name__}")

        if not isinstance(dest, str):
            raise ValueError(f"'dest' must be string, got {type(dest).__name__}")

        if not isinstance(peer, str):
            raise ValueError(f"'peer' must be string, got {type(peer).__name__}")

        if not isinstance(startb, int):
            raise ValueError(f"'startb' must ben integer, got {type(startb).__name__}")

        if not isinstance(endb, int):
            raise ValueError(f"'endb' must ben integer, got {type(endb).__name__}")

        try:
            if not is_path_within_cwd(file) or not is_path_within_cwd(dest):
                raise ValueError("Files not within working directory")

            if startb == -1:
                if os.path.exists(dest):
                    startb = os.stat(dest).st_size
                else:
                    startb = 0

            if peer not in self.cluster.peers.get_established():
                raise OfflinePeer(peer)

            result, responses = await self.cluster.send_command(
                f"FILEGET {startb} {endb} {file}", peer, timeout=10.0, raise_err=False
            )
            if not result:
                raise FileGetException(responses[peer])

            fn, fmeta, fdata = responses[peer].split(" ")

            if not fn == file:
                raise FileGetException("File mismatch")

            os.makedirs(os.path.dirname(dest), exist_ok=True)
            payload = zlib.decompress(base64.b64decode(fdata))

            if not os.path.exists(dest):
                open(dest, "w+b").close()

            apply_meta(dest, fmeta)

            with open(dest, "r+b") as f:
                f.seek(startb)
                f.write(payload)
                if endb == -1:
                    f.truncate()

        except FileGetException:
            raise
        except Exception as e:
            raise FileGetException(e)

    async def fileput(self, file: str, dest: str, peer: str):
        if not isinstance(file, str):
            raise ValueError(f"'file' must be string, got {type(file).__name__}")

        if not isinstance(dest, str):
            raise ValueError(f"'dest' must be string, got {type(dest).__name__}")

        if not isinstance(peer, str):
            raise ValueError(f"'peer' must be string, got {type(peer).__name__}")

        try:
            if (
                not is_path_within_cwd(file)
                or not is_path_within_cwd(dest)
                or not os.path.exists(file)
            ):
                raise ValueError("Invalid file input or destination")

            await self.cluster.send_command(f"FILEPUT {file} {dest}", peer)

        except Exception as e:
            raise FilePutException(e)

    async def folderput(self, folder: str, in_background: bool = True):
        if not isinstance(folder, str):
            raise ValueError(f"'folder' must be string, got {type(folder).__name__}")

        if not isinstance(in_background, bool):
            raise ValueError(
                f"'in_background' must be boolean, got {type(in_background).__name__}"
            )

        def _list_real_files(folder):
            for root, dirs, files in os.walk(folder, followlinks=False):
                dirs[:] = [d for d in dirs if not os.path.islink(os.path.join(root, d))]
                for name in files:
                    path = os.path.join(root, name)
                    if os.path.isfile(path) and not os.path.islink(path):
                        yield path

        async def _send_file_to_peer(peer, file):
            try:
                async with sem:
                    await self.fileput(file, file, peer)
            except FilePutException as e:
                logger.warning(f"Cannot send to {peer}: {e}")

        if not is_path_within_cwd(folder):
            raise ValueError("Folder not within working directory")

        sem = asyncio.Semaphore(20)

        files = [f for f in _list_real_files(folder)]
        peers = list(self.cluster.peers.get_established())

        if in_background:
            for file in files:
                for peer in peers:
                    t = asyncio.create_task(_send_file_to_peer(peer, file))
                    t.add_done_callback(
                        lambda _t: _t.exception() and logger.critical(_t.exception())
                    )
            return

        tasks = [
            asyncio.create_task(_send_file_to_peer(peer, file))
            for file in files
            for peer in peers
        ]

        await asyncio.gather(*tasks, return_exceptions=False)
