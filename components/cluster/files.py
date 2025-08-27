import asyncio
import glob
import base64
import os
import zlib

from .exceptions import *
from components.logs import logger
from components.utils import is_path_within_cwd, apply_meta
from components.models import validate_call


class Files:
    def __init__(self, cluster: "Server"):
        self.cluster = cluster

    @validate_call
    async def filedel(self, file: str, peer: str):
        try:
            if not is_path_within_cwd(file):
                raise ValueError("File not within working directory")

            if peer not in self.cluster.peers.get_established():
                raise UnknownPeer(peer)

            async with self.cluster.receiving:
                sent = await self.cluster.send_command(f"FILEDEL {file}", peer)
                await self.cluster.await_receivers(sent, raise_err=True)

        except Exception as e:
            raise FileDelException(e)

    @validate_call
    async def fileget(
        self,
        file: str,
        dest: str,
        peer: str,
        startb: int = 0,
        endb: int = -1,
    ):
        try:
            if not is_path_within_cwd(file) or not is_path_within_cwd(dest):
                raise ValueError("Files not within working directory")

            if startb == -1:
                if os.path.exists(dest):
                    startb = os.stat(dest).st_size
                else:
                    startb = 0

            if peer not in self.cluster.peers.get_established():
                raise UnknownPeer(peer)

            async with self.cluster.receiving:
                sent = await self.cluster.send_command(
                    f"FILEGET {startb} {endb} {file}", peer
                )
                result, responses = await self.cluster.await_receivers(
                    sent, raise_err=False
                )

            if not result:
                raise FileGetException(responses)

            fn, fmeta, fdata = responses[peer].split(" ")

            if not fn == file:
                raise ValueError("File mismatch")

            os.makedirs(os.path.dirname(dest), exist_ok=True)
            payload = zlib.decompress(base64.b64decode(fdata))

            if not os.path.exists(dest):
                open(dest, "w+b").close()

            apply_meta(dest, fmeta)

            with open(dest, "r+b") as f:
                f.seek(startb)
                f.write(payload)

        except Exception as e:
            raise FileGetException(e)

    @validate_call
    async def fileput(
        self,
        file: str,
        dest: str,
        peer: str,
    ):
        try:
            if (
                not is_path_within_cwd(file)
                or not is_path_within_cwd(dest)
                or not os.path.exists(file)
            ):
                raise ValueError("Invalid file input or destination")

            if peer not in self.cluster.peers.get_established():
                raise UnknownPeer(peer)

            async with self.cluster.receiving:
                sent = await self.cluster.send_command(f"FILEPUT {file} {dest}", peer)
                await self.cluster.await_receivers(sent, raise_err=True)

        except Exception as e:
            raise FilePutException(e)

    async def sync_folder(self, folder: str, in_background: bool = True):
        if not is_path_within_cwd(folder):
            raise ValueError("Folder not within working directory")

        sem = asyncio.Semaphore(20)

        async def send_file_to_peer(peer, file):
            try:
                async with sem:
                    await self.fileput(file, file, peer)
            except FilePutException as e:
                logger.warning(f"Cannot send to {peer}: {e}")

        files = glob.glob(f"{folder}/*")
        peers = list(self.cluster.peers.get_established())

        if in_background:
            for file in files:
                for peer in peers:
                    t = asyncio.create_task(send_file_to_peer(peer, file))
                    t.add_done_callback(
                        lambda _t: _t.exception() and logger.critical(_t.exception())
                    )
            return

        tasks = [
            asyncio.create_task(send_file_to_peer(peer, file))
            for file in files
            for peer in peers
        ]

        await asyncio.gather(*tasks, return_exceptions=False)
