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
    mime_type: str
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

        self.mime_type = to_str(self.mime_type.strip())
        if not self.mime_type:
            raise ValueError("mime_type", "'mime_type' must be a non-empty string")

    @classmethod
    async def create_from_bytes(cls, data: bytes, **kwargs) -> "Asset":
        from components.utils.images import convert_image_to_webp

        _fwrite = False
        if not isinstance(data, bytes):
            raise ValueError("'data' must be bytes")

        id_ = validate_uuid_str(kwargs.get("id", str(uuid4())))
        mime_type = mime.from_buffer(data)  # do not rely on user input
        filename = kwargs.pop("filename", id_)
        overlay = kwargs.pop("overlay", None)
        cluster = kwargs.pop("cluster", None)
        compress = kwargs.pop("compress", False)
        asset = Asset(id=id_, mime_type=mime_type, filename=filename, overlay=overlay)
        asset_path = f"assets/{asset.filename}"

        if compress:
            if mime_type.startswith("image/") and mime_type != "image/webp":
                _fwrite = True
                await asyncio.to_thread(
                    convert_image_to_webp,
                    image=data,
                    save_as=asset_path,
                    quality=100,
                )
        if not _fwrite:
            with open(asset_path, "wb") as f:
                f.write(data)

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
