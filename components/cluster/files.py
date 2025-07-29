import base64
import os
import zlib

from .exceptions import *
from components.utils import is_path_within_cwd
from components.utils.files import apply_meta
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
            raise FileDelException(Exception)

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
            raise FileGetException(Exception)

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
            raise FilePutException(Exception)
