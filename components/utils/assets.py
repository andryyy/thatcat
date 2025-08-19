import magic
import os

from components.cluster.exceptions import (
    FileGetException,
    FileDelException,
    FilePutException,
)
from components.logs import logger
from components.models.assets import Asset, UUID, uuid4, validate_call


@validate_call
async def request_asset(
    cluster: object, asset_uuid: UUID, peer: str | list = []
) -> bool:
    from components.utils.misc import ensure_list
    from components.utils.images import convert_file_to_webp

    asset_id = str(asset_uuid)
    if os.path.exists(f"assets/{asset_id}"):
        return True
    for peer in ensure_list(peer) or cluster.peers.get_established():
        try:
            await cluster.files.fileget(
                f"assets/{asset_id}", f"assets/{asset_id}", peer
            )
            convert_file_to_webp(f"assets/{asset_id}")
            logger.success(f"Requested {asset_id} from {peer}")
            return True
        except FileGetException as e:
            logger.warning(f"Cannot request asset from {peer}: {e}")
    else:
        return False


@validate_call
async def remove_asset(
    cluster: object, asset_uuid: UUID, remote_only: bool = False
) -> None:
    asset_id = str(asset_uuid)
    asset_file = f"assets/{asset_id}"
    for peer in cluster.peers.get_established():
        try:
            await cluster.files.filedel(asset_file, peer)
            logger.success(f"Removed {asset_id} from {peer}")
        except FileDelException as e:
            logger.warning(f"Cannot remove asset from {peer}: {e}")

    if not remote_only:
        if os.path.exists(asset_file):
            try:
                os.remove(asset_file)
                logger.success(f"Removed {asset_id} locally")
            except Exception as e:
                raise FileDelException(e)


@validate_call
async def push_asset(cluster: object, asset_uuid: UUID) -> None:
    asset_id = str(asset_uuid)
    asset_file = f"assets/{asset_id}"
    for peer in cluster.peers.get_established():
        try:
            await cluster.files.fileput(asset_file, asset_file, peer)
            logger.success(f"Sent {asset_id} to {peer}")
        except FilePutException as e:
            logger.warning(f"Cannot send asset to {peer}: {e}")
