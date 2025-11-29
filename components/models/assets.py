import os
import asyncio

from components.models.helpers import validate_uuid_str, to_str
from dataclasses import dataclass
from magic import Magic
from uuid import uuid4
from werkzeug.utils import secure_filename

mime = Magic(mime=True)


@dataclass
class Asset:
    id: str
    filename: str | None = None
    overlay: str | None = None

    def __post_init__(self):
        self.id = validate_uuid_str(self.id)

        if self.filename:
            self.filename = secure_filename(to_str(self.filename.strip())) or self.id
        else:
            self.filename = self.id

        if self.overlay:
            self.overlay = to_str(self.overlay.strip()) or None

    @property
    def mime_type(self) -> str:
        try:
            return mime.from_file(f"assets/{self.id}")
        except Exception:
            return "application/octet-stream"

    def as_bytes(self) -> bytes:
        asset_path = f"assets/{self.id}"
        with open(asset_path, "rb") as f:
            return f.read()

    @classmethod
    async def from_bytes(cls, data_bytes: bytes, **kwargs) -> "Asset":
        from components.utils.images import convert_image_to_webp
        from components.logs import logger

        if not isinstance(data_bytes, bytes):
            raise ValueError("'data_bytes' must be bytes")

        asset_id = validate_uuid_str(kwargs.get("id", str(uuid4())))
        asset_path = f"assets/{asset_id}"

        with open(asset_path, "wb") as f:
            f.write(data_bytes)

        filename = kwargs.pop("filename", asset_id)
        overlay = kwargs.pop("overlay", None)
        cluster = kwargs.pop("cluster", None)  # Offline peers will request on demand
        compress = kwargs.pop("compress", False)

        asset = Asset(id=asset_id, filename=filename, overlay=overlay)

        if compress:
            loseless = kwargs.pop("loseless", True)
            quality = kwargs.pop("quality", 90)
            try:
                if (
                    asset.mime_type.startswith("image/")
                    and asset.mime_type != "image/webp"
                ):
                    compressed_bytes = await asyncio.to_thread(
                        convert_image_to_webp,
                        image=data_bytes,
                        quality=quality,
                        loseless=loseless,
                    )
                    with open(asset_path, "wb") as f:
                        f.write(compressed_bytes)
            except Exception as e:
                logger.warning(
                    f"Failed to compress image {asset.filename} ({asset.id}): {e}"
                )

        if cluster:
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
