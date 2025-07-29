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
