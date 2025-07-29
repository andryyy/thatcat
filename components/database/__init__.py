import contextvars
import os
import shutil
import json

from aiotinydb import AIOTinyDB as TinyDB
from aiotinydb.storage import AIOStorage
from copy import copy
from tinydb import Query
from tinydb.table import Document
from components.utils import is_path_within_cwd

__all__ = [
    "TinyDB",
    "Query",
    "Document",
    "TINYDB_PARAMS",
    "IN_MEMORY_DB",
    "CTX_LOCK_ID",
    "dbcommit",
    "Q",
    "dbparams",
]

TinyDB.DEFAULT_TABLE_KWARGS = {"cache_size": 0}
TINYDB_PARAMS = {
    "filename": "database/main",
    "indent": 2,
    "sort_keys": True,
}
IN_MEMORY_DB = dict()
IN_MEMORY_DB["PATCHED_TABLES"] = dict()
IN_MEMORY_DB["SESSION_VALIDATED"] = dict()
IN_MEMORY_DB["QUEUED_USER_TASKS"] = dict()
IN_MEMORY_DB["WS_CONNECTIONS"] = dict()
IN_MEMORY_DB["ASSET_TICKETS"] = dict()
IN_MEMORY_DB["CACHE"] = {
    "FORMS": dict(),
    "LOCATIONS": dict(),
}
IN_MEMORY_DB["PROMOTE_USERS"] = set()
IN_MEMORY_DB["TOKENS"] = {
    "REGISTER": dict(),
    "LOGIN": dict(),
}

CTX_LOCK_ID = contextvars.ContextVar("CTX_LOCK_ID", default=None)
Q = Query()


if not os.path.exists("database/main") or os.path.getsize("database/main") == 0:
    os.makedirs(os.path.dirname("database/main"), exist_ok=True)
    with open("database/main", "w") as f:
        f.write("{}")

os.chmod("database/main", 0o600)


def dbparams(lock_id: float | str | None = None):
    default_params = copy(TINYDB_PARAMS)
    lock_id = lock_id or CTX_LOCK_ID.get()

    if lock_id:
        dbfile = f"database/main.{lock_id}"
        if not os.path.exists(dbfile):
            shutil.copy("database/main", dbfile)
    else:
        dbfile = "database/main"

    default_params["filename"] = dbfile
    return default_params


async def dbcommit(commit_tables: set, ticket: str | None = None) -> None:
    assert commit_tables
    db_params = dbparams(ticket)

    with open(db_params["filename"], "r") as f:
        modified_db = json.load(f)

    async with TinyDB(**TINYDB_PARAMS) as db:
        current_db = json.load(db._storage._handle)
        for t in commit_tables:
            current_db[t] = modified_db[t]
        db._storage._handle.seek(0)
        serialized = json.dumps(current_db, **db._storage.kwargs)
        db._storage._handle.write(serialized)
        db._storage._handle.flush()
        db._storage._handle.truncate()
        db.clear_cache()
        os.unlink(db_params["filename"])
