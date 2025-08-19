import asyncio
import glob
import os
import stat


def export_meta(filepath: str) -> str:
    st = os.stat(filepath)
    mode = stat.S_IMODE(st.st_mode)
    mtime = int(st.st_mtime)
    return f"{mode:04o}.{mtime:x}"


def apply_meta(filepath: str, meta_str: str):
    mode_str, mtime_str = meta_str.split(".")
    mode = int(mode_str, 8)
    mtime = int(mtime_str, 16)
    os.chmod(filepath, mode)
    atime = os.stat(filepath).st_atime
    os.utime(filepath, (atime, mtime))


async def sync_folder(folder: str, in_background: bool = True):
    from components.cluster import cluster
    from components.logs import logger
    from components.utils import is_path_within_cwd

    if not is_path_within_cwd(folder):
        raise ValueError("Folder not within working directory")

    sem = asyncio.Semaphore(20)

    async def send_file_to_peer(peer, file):
        try:
            async with sem:
                await cluster.files.fileput(file, file, peer)
        except FilePutException as e:
            logger.warning(f"Cannot send to {peer}: {e}")

    files = glob.glob(f"{folder}/*")
    peers = list(cluster.peers.get_established())

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
