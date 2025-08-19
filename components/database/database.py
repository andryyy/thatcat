import contextvars
import os
import shutil
import json

from aiotinydb import AIOTinyDB as TinyDB
from copy import copy
from tinydb import Query
from tinydb.table import Document
from tinydb.utils import LRUCache

__all__ = [
    "TinyDB",
    "Query",
    "Document",
    "LRUCache",
    "TINYDB_PARAMS",
    "CTX_LOCK_ID",
    "dbcommit",
    "Q",
    "dbparams",
]

TINYDB_PARAMS = {
    "filename": "database/main",
    "indent": 2,
    "sort_keys": True,
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
        os.unlink(db_params["filename"])
