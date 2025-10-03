import os

from PIL import Image, UnidentifiedImageError
from components.models.helpers import *
from dataclasses import dataclass
from magic import Magic
from shutil import SameFileError, copy2
from uuid import uuid4
from werkzeug.utils import secure_filename

mime = Magic(mime=True)


@dataclass
class Asset:
    id: str
    filename: str | None = None
    mime_type: str | None = None

    def __post_init__(self):
        self.id = validate_uuid_str(self.id)

        if self.filename:
            if not isinstance(self.filename, str) or self.filename == "":
                raise ValueError("'filename' must be a non-empty string or None")

        if self.mime_type:
            if not isinstance(self.mime_type, str) or self.mime_type == "":
                raise ValueError("'filename' must be a non-empty string or None")
        else:
            self.mime_type = mime.from_file(os.path.join("assets", self.id))

        if not self.filename:
            self.filename = self.id
        else:
            self.filename = secure_filename(self.filename)

        if self.mime_type.startswith("image/") and self.mime_type != "image/webp":
            self._convert_image_to_webp()
            self.mime_type = "image/webp"

    @classmethod
    async def create_from_bytes(cls, data: bytes, **kwargs) -> "Asset":
        from components.cluster import cluster

        asset_id = validate_uuid_str(kwargs.get("id", str(uuid4())))
        asset_path = f"assets/{asset_id}"

        if not isinstance(data, bytes):
            raise ValueError("'data' must be bytes")

        with open(asset_path, "wb") as f:
            f.write(data)

        asset = Asset(id=asset_id, **kwargs)

        ok_peers = set()
        for peer in cluster.peers.get_established():
            try:
                await cluster.files.fileput(asset_path, asset_path, peer)
                ok_peers.add(peer)
            except:
                os.unlink(asset_path)
                for peer in ok_peers:
                    await cluster.files.filedel(asset_path, peer)
                raise

        return asset

    @classmethod
    async def create_from_file(cls, filename: str, **kwargs) -> "Asset":
        from components.cluster import cluster
        from components.utils.misc import is_path_within_cwd

        asset_id = validate_uuid_str(kwargs.get("id", str(uuid4())))
        asset_path = f"assets/{asset_id}"

        if not isinstance(filename, str):
            raise ValueError("'filename' must be str")

        if not is_path_within_cwd(filename):
            raise ValueError("file is not within cwd")

        try:
            copy2(filename, asset_path)
            same_file = False
        except SameFileError:
            same_file = True

        asset = Asset(id=asset_id, **kwargs)

        ok_peers = set()
        for peer in cluster.peers.get_established():
            try:
                await cluster.files.fileput(asset_path, asset_path, peer)
                ok_peers.add(peer)
            except:
                if not same_file:
                    os.unlink(asset_path)
                for peer in ok_peers:
                    await cluster.files.filedel(asset_path, peer)
                raise

        return asset

    def _convert_image_to_webp(self, max_width: int = 0, quality: int = 85) -> None:
        with Image.open(f"assets/{self.id}") as img:
            width, height = img.size

            if max_width and width > max_width:
                new_height = int(max_width * height / width)
                img = img.resize((max_width, new_height), Image.LANCZOS)

            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            copy2(f"assets/{self.id}", f"assets/{self.id}.original")

            img.save(f"assets/{self.id}", format="WEBP", quality=quality)
