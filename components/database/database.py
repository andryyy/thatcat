from __future__ import annotations

import asyncio, json, zlib, base64, contextvars

from collections import OrderedDict
from components.cluster.exceptions import ClusterException
from components.database.helpers import (
    create_sort_key,
    filter_rows,
    get_all,
    match_clause,
    merge_dict,
    paginate_rows,
)
from components.logs import logger
from components.utils.misc import ensure_list
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


try:
    import msgpack
except Exception:
    msgpack = None

JSON = Dict[str, Any]

# Constants
DEFAULT_CACHE_SIZE = 2048
DEFAULT_PAGE_SIZE = 50
MAX_COMPRESSED_PAYLOAD_SIZE = 32 * 1024 * 1024  # 32 MB
MAX_RAW_PAYLOAD_SIZE = 128 * 1024 * 1024  # 128 MB
SYNC_PAYLOAD_FORMAT_VERSION = 2

# Table field mappings for list_rows projection
_DEFAULT_LIST_ROW_FIELDS = ["id", "created", "updated", "doc_version"]
LIST_ROW_FIELDS = {
    "users": _DEFAULT_LIST_ROW_FIELDS + ["login"],
    "projects": _DEFAULT_LIST_ROW_FIELDS + ["name", "assigned_users"],
    "cars": _DEFAULT_LIST_ROW_FIELDS + ["vin", "assigned_users", "assigned_project"],
    "processings": _DEFAULT_LIST_ROW_FIELDS + ["assigned_user"],
}

# Index definitions for each table
INDEX_FIELDS = {
    "cars": ["id", "vin", "assigned_users", "assigned_project"],
    "projects": ["id", "name", "assigned_users"],
    "users": ["id", "login", "credentials.id", "acl"],
    "processings": ["id", "assigned_user"],
}

_changed_ctx = contextvars.ContextVar("_changed_ctx", default={})
_deleted_ctx = contextvars.ContextVar("_deleted_ctx", default={})
_locks_ctx = contextvars.ContextVar("_locks_ctx", default={})
_snapshots_ctx = contextvars.ContextVar("_snapshots_ctx", default={})


def _reset_context_vars():
    _changed_ctx.set({})
    _deleted_ctx.set({})
    _locks_ctx.set({})
    _snapshots_ctx.set({})


class StorageCodec:
    """Codec for serializing/deserializing database documents."""

    SUPPORTED_CODECS = {"json", "msgpack"}

    def __init__(self, kind: str = "msgpack"):
        kind = (kind or "msgpack").lower()
        if kind not in self.SUPPORTED_CODECS:
            raise ValueError(f"codec must be one of {self.SUPPORTED_CODECS}")
        if kind == "msgpack" and msgpack is None:
            raise RuntimeError(
                "MessagePack codec requested but 'msgpack' is not installed."
            )
        self.kind = kind

    def dumps(self, obj: dict) -> bytes:
        """Serialize an object to bytes."""
        if self.kind == "msgpack":
            return msgpack.dumps(obj, use_bin_type=True)
        elif self.kind == "json":
            return json.dumps(obj, ensure_ascii=False).encode("utf-8")
        raise ValueError(f"Unknown codec: {self.kind}")

    def loads(self, data: bytes) -> dict:
        """Deserialize bytes to an object."""
        if self.kind == "msgpack":
            return msgpack.loads(data, raw=False)
        elif self.kind == "json":
            return json.loads(data.decode("utf-8"))
        raise ValueError(f"Unknown codec: {self.kind}")


def _requires_cluster(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._cluster_ready.is_set():
            raise ClusterException("Cluster not set")
        elif not self.cluster.peers.local.leader:
            raise ClusterException("Cluster not ready")
        return await func(self, *args, **kwargs)

    return wrapper


class _LRU:
    def __init__(self, max_entries: int = DEFAULT_CACHE_SIZE):
        self.max = max_entries
        self.od: "OrderedDict[tuple[str,str], JSON]" = OrderedDict()

    def get(self, key):
        if key in self.od:
            self.od.move_to_end(key)
            return self.od[key]
        return None

    def put(self, key, val):
        self.od[key] = val
        self.od.move_to_end(key)
        if len(self.od) > self.max:
            self.od.popitem(last=False)

    def delete(self, key):
        self.od.pop(key, None)

    def __contains__(self, key):
        return key in self.od


class Database:
    def __init__(
        self, base: str, main_file: str = "main.json", *, codec: str = "msgpack"
    ):
        self.base = Path(base)
        self.base.mkdir(parents=True, exist_ok=True)
        self.main_path = self.base / main_file
        self.cluster = None
        self._cluster_ready = asyncio.Event()
        self._open = False
        self._manifest: JSON = {}
        self._peer_manifests: Dict[str, JSON] = {}
        self._indexes: Dict[str, Dict[str, Dict[Any, Set[str]]]] = {}
        self._cache = _LRU(max_entries=DEFAULT_CACHE_SIZE)
        self._locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._codec = StorageCodec(codec)
        if self.main_path.exists():
            self._manifest = json.loads(self.main_path.read_text(encoding="utf-8"))
        else:
            self._manifest = {"tables": {}}

        if not _changed_ctx.get():
            _changed_ctx.set({})
        if not _deleted_ctx.get():
            _deleted_ctx.set({})

    @property
    def cluster(self):
        return self._cluster

    @cluster.setter
    def cluster(self, value):
        self._cluster = value
        if value is not None:
            self._cluster_ready.set()

    def _validate_id(self, id_: str) -> None:
        """Validate that a document ID is safe and well-formed."""
        if not id_:
            raise ValueError("id must be non‑empty")
        if not isinstance(id_, str):
            raise TypeError("id must be str")
        if "/" in id_ or "\\" in id_ or id_.startswith("."):
            raise ValueError(f"Invalid document id {id_!r}")

    def _resolve_doc_path(self, table: str, id_: str) -> Path:
        """Resolve and validate the file path for a document."""
        self._validate_id(id_)
        tdir = (self.base / table).resolve()
        p = (self.base / table / id_).resolve()
        if p.parent != tdir:
            raise ValueError(f"Invalid document id {id_!r}")
        return p

    def _lock_for(self, table: str, id_: str) -> asyncio.Lock:
        """Get or create a lock for a specific document."""
        key = (table, id_)
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def do_rollback(self, cluster_peers: list = []) -> None:
        """Rollback changes to previously saved snapshots."""
        try:
            snapshots = _snapshots_ctx.get()
            if not snapshots:
                logger.debug("Nothing to rollback")
                return

            # Restore local snapshots
            for table in snapshots:
                await self.apply_snapshot(table, snapshots[table])

            # Propagate rollback to peers if needed
            if cluster_peers:
                comp_sync = await self.make_sync_from_docs(snapshots)
                _, comp_b64 = comp_sync.split(" ", 1)
                for peer in cluster_peers:
                    await self.cluster.send_command(
                        f"DBSYNC LAZY {comp_b64}", peer, raise_err=True
                    )
        except Exception as e:
            logger.critical(e)
            logger.error("Rollback failed")

    async def _build_all_indexes(self) -> None:
        """Build all indexes defined in INDEX_FIELDS constant."""
        for table, fields in INDEX_FIELDS.items():
            await self.build_index(table, fields)

    async def __aenter__(self):
        _reset_context_vars()
        await self._build_all_indexes()
        return self

    async def _replicate_to_peers(self, sync_str: str) -> None:
        """Replicate changes to cluster peers, rolling back on failure."""
        try:
            _, b64 = sync_str.split(" ", 1)
            ok_peers = []
            failed = False

            for peer in self.cluster.peers.get_established():
                result, _ = await self.cluster.send_command(
                    f"DBSYNC {b64}", peer, raise_err=False
                )
                if result:
                    ok_peers.append(peer)
                else:
                    failed = True

            if failed:
                await asyncio.shield(self.do_rollback(ok_peers))
                logger.error(
                    "Replication failed on at least one peer; rolled back everywhere."
                )
        except Exception as e:
            logger.critical(e)

    async def _cleanup_locks_and_save(self) -> None:
        """Release cluster locks and save manifest."""
        async with self._lock_for("aexit", "1"):
            locks = _locks_ctx.get()
            for doc_id, lock_id in locks.items():
                await asyncio.shield(self.cluster.release(lock_id, [doc_id]))
            self.main_path.write_text(json.dumps(self._manifest, indent=2))

    async def __aexit__(self, exc_type, exc, tb):
        sync_str = await self.sync_out()

        if sync_str and not exc:
            await self._replicate_to_peers(sync_str)
        elif exc:
            logger.error(f"Rolling back local changes due to error: {exc}.")
            await asyncio.shield(self.do_rollback())

        await self._cleanup_locks_and_save()

    def _changed_dict(self) -> dict:
        return _changed_ctx.get()

    def _deleted_dict(self) -> dict:
        return _deleted_ctx.get()

    def _tbl(self, table: str) -> Dict[str, Any]:
        return self._manifest.setdefault("tables", {}).setdefault(
            table, {"doc_versions": {}}
        )

    def table_version(self, table: str) -> int:
        t = self._tbl(table)
        return sum(int(v) for v in t["doc_versions"].values())

    def doc_version(self, table: str, id_: str) -> int:
        return int(self._tbl(table)["doc_versions"].get(id_, 0))

    def ids(self, table: str) -> List[str]:
        tdir = self.base / table
        if not tdir.exists():
            return []
        return sorted(
            [
                p.name
                for p in tdir.iterdir()
                if p.is_file() and not p.name.endswith(".tmp")
            ]
        )

    async def get(self, table: str, id_: str) -> Optional[JSON]:
        key = (table, id_)
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        lock = self._lock_for(table, id_)
        async with lock:
            cached = self._cache.get(key)
            if cached is not None:
                return cached
            path = self._resolve_doc_path(table, id_)
            if not path.exists():
                return None
            data = await asyncio.to_thread(path.read_bytes)
            doc = self._codec.loads(data)
            if isinstance(doc, dict):
                doc.setdefault("id", id_)
                doc.setdefault("doc_version", self.doc_version(table, id_))
            self._cache.put(key, doc)
            return doc

    async def _read_disk_nocache(self, table: str, id_: str) -> Optional[JSON]:
        path = self._resolve_doc_path(table, id_)
        if not path.exists():
            return None
        data = await asyncio.to_thread(path.read_bytes)
        doc = self._codec.loads(data)
        if isinstance(doc, dict):
            doc.setdefault("id", id_)
            doc.setdefault("doc_version", self.doc_version(table, id_))
        return doc

    async def upsert(
        self,
        table: str,
        id_: str,
        doc: JSON,
        *,
        replace: bool = True,
        base_version: Optional[int] = 0,
    ) -> None:
        if base_version != 0:
            if self.doc_version(table, id_) > base_version:
                raise ValueError("Document changed, please reload the form")
        await self._do_ops(
            table,
            kind="upsert",
            id_=id_,
            doc=doc,
            replace=replace,
            incoming_version=None,
        )

    async def patch(
        self,
        table: str,
        id_: str,
        changes: JSON,
        base_version: Optional[int] = 0,
    ) -> None:
        if base_version != 0:
            if self.doc_version(table, id_) > base_version:
                raise ValueError("Document changed, please reload the form")
        await self._do_ops(
            table,
            kind="patch",
            id_=id_,
            doc=changes,
            replace=False,
            incoming_version=None,
        )

    async def delete(
        self,
        table: str,
        id_: str,
    ) -> None:
        await self._do_ops(
            table,
            kind="delete",
            id_=id_,
            doc=None,
            replace=False,
            incoming_version=None,
        )

    def _update_indexes_for_doc_change(
        self, table: str, *, id_: str, old_doc: Optional[JSON], new_doc: Optional[JSON]
    ) -> None:
        """Update indexes when a document changes."""
        if table not in self._indexes:
            return

        for f, mapping in self._indexes[table].items():
            # Remove old document values from index
            if old_doc is not None:
                for v in get_all(old_doc, f):
                    try:
                        s = mapping.get(v)
                    except TypeError:
                        logger.warning(
                            f"Cannot index unhashable value of type {type(v).__name__} "
                            f"for field '{f}' in table '{table}'"
                        )
                        continue
                    if s:
                        s.discard(id_)
                        if not s:
                            mapping.pop(v, None)

            # Add new document values to index
            if new_doc is not None:
                for v in get_all(new_doc, f):
                    try:
                        bucket = mapping.setdefault(v, set())
                    except TypeError:
                        logger.warning(
                            f"Cannot index unhashable value of type {type(v).__name__} "
                            f"for field '{f}' in table '{table}'"
                        )
                        continue
                    bucket.add(id_)

    async def _upsert_local(
        self,
        table: str,
        id_: str,
        doc: JSON,
        *,
        replace: bool,
        incoming_version: Optional[int] = None,
    ) -> None:
        lock = self._lock_for(table, id_)
        async with lock:
            old_doc = await self._read_disk_nocache(table, id_)
            to_write = (
                doc
                if replace
                else (merge_dict(old_doc, doc) if old_doc is not None else doc)
            )

            tdir = self.base / table
            tdir.mkdir(parents=True, exist_ok=True)
            if isinstance(to_write, dict):
                to_write = {
                    k: v
                    for k, v in to_write.items()
                    if k != "id" and k != "doc_version"
                }
            data = self._codec.dumps(to_write)
            path = self._resolve_doc_path(table, id_)
            tmp = path.with_suffix(path.suffix + ".tmp")
            await asyncio.to_thread(tmp.write_bytes, data)
            await asyncio.to_thread(tmp.replace, path)

            t = self._tbl(table)
            if incoming_version is not None:
                t["doc_versions"][id_] = incoming_version
            else:
                t["doc_versions"][id_] = int(t["doc_versions"].get(id_, 0)) + 1

            changed = self._changed_dict().setdefault(table, set())
            changed.add(id_)
            deleted = self._deleted_dict().setdefault(table, set())
            deleted.discard(id_)

            cached_doc = to_write
            if isinstance(cached_doc, dict):
                cached_doc = dict(cached_doc)
                cached_doc["id"] = id_
                cached_doc["doc_version"] = t["doc_versions"][id_]

            self._cache.put((table, id_), cached_doc)

            self._update_indexes_for_doc_change(
                table, id_=id_, old_doc=old_doc, new_doc=cached_doc
            )

    async def _delete_local(self, table: str, id_: str) -> None:
        lock = self._lock_for(table, id_)
        async with lock:
            path = self._resolve_doc_path(table, id_)
            if path.exists():
                await asyncio.to_thread(path.unlink)

            t = self._tbl(table)
            if id_ in t["doc_versions"]:
                del t["doc_versions"][id_]

            deleted = self._deleted_dict().setdefault(table, set())
            deleted.add(id_)
            changed = self._changed_dict().setdefault(table, set())
            changed.discard(id_)

            self._cache.delete((table, id_))

            if table in self._indexes:
                for mapping in self._indexes[table].values():
                    empties = []
                    for v, s in mapping.items():
                        s.discard(id_)
                        if not s:
                            empties.append(v)
                    for v in empties:
                        mapping.pop(v, None)

            self._locks.pop((table, id_), None)

    async def build_index(self, table: str, fields: List[str]) -> None:
        """Build indexes for specified fields in a table."""
        idxs = self._indexes.setdefault(table, {})

        # Initialize index structures
        for f in fields:
            idxs[f] = {}

        # Populate indexes with existing documents
        for id_ in self.ids(table):
            doc = await self.get(table, id_)
            if not doc:
                continue

            for f in fields:
                for v in get_all(doc, f):
                    try:
                        bucket = idxs[f].setdefault(v, set())
                        bucket.add(id_)
                    except TypeError:
                        logger.warning(
                            f"Cannot index unhashable value of type {type(v).__name__} "
                            f"for field '{f}' in table '{table}'"
                        )

    async def search(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> List[JSON]:
        """Search for documents matching a where clause.

        Uses indexes when available, auto-creates indexes for unindexed fields.
        Returns full documents (unlike list_rows which returns projections).

        Args:
            table: Table name
            where: Filter clause (AND across fields, OR within field for lists)
            limit: Maximum number of results to return
        """
        where = where or {}
        idxs = self._indexes.get(table, {})
        candidate_ids: Optional[Set[str]] = None
        unindexed_fields = []

        # Use indexes to narrow down candidates
        for f, val in where.items():
            values = ensure_list(val)
            if f in idxs:
                # Use existing index
                field_results = set()
                for v in values:
                    field_results |= idxs[f].get(v, set())
                if candidate_ids is None:
                    candidate_ids = field_results
                else:
                    candidate_ids &= field_results
            else:
                # Track unindexed fields for auto-indexing
                unindexed_fields.append(f)

        # Auto-create indexes for unindexed fields
        if unindexed_fields:
            logger.info(
                f"Auto-creating indexes for fields {unindexed_fields} in table '{table}'"
            )
            await self.build_index(table, unindexed_fields)

        # If no indexes were used, search all documents
        if candidate_ids is None:
            candidate_ids = set(self.ids(table))

        # Filter candidates using the where clause
        results: Set[str] = set()
        for id_ in candidate_ids:
            doc = await self.get(table, id_)
            if not doc:
                continue
            if match_clause(doc, where):
                results.add(id_)

        # Sort and limit results
        ordered = sorted(results)
        if limit is not None:
            ordered = ordered[:limit]

        # Fetch final documents
        out: List[JSON] = []
        for id_ in ordered:
            doc = await self.get(table, id_)
            if doc:
                out.append(doc)
        return out

    async def list_rows(
        self,
        table: str,
        *,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,  # -1 => return all rows
        sort_attr: str | int = "id",  # -1 => no sort
        sort_reverse: bool = False,
        where: dict | None = None,
        any_of: list[dict] | None = None,
        q: str | None = None,
    ):
        """
        Filtering:
          - where: AND across fields; within a field pass a list for OR (e.g., {"status": ["open","pending"]})
          - any_of: OR across clauses, each clause has same semantics as 'where'
          - q: case-insensitive substring applied to stringified row values

        Notes:
          - Filtering happens on the *row projection* (first value at each path).
        """

        # Load and project rows
        ids = self.ids(table)
        rows = []
        for id_ in ids:
            doc = await self.get(table, id_)
            if not doc:
                continue
            rows.append(
                {
                    k: v
                    for k, v in doc.items()
                    if k in LIST_ROW_FIELDS.get(table, _DEFAULT_LIST_ROW_FIELDS)
                }
            )

        # Apply filters
        rows = filter_rows(rows, where, any_of, q)

        # Apply sorting
        if sort_attr != -1:
            rows.sort(
                key=create_sort_key(sort_attr, sort_reverse), reverse=sort_reverse
            )

        # Apply pagination
        return paginate_rows(rows, page, page_size, sort_attr, sort_reverse)

    def _has_pending_changes(self) -> bool:
        """Check if there are any pending changes or deletions."""
        changed = self._changed_dict()
        deleted = self._deleted_dict()
        has_changes = any(len(s) > 0 for s in changed.values())
        has_deletes = any(len(s) > 0 for s in deleted.values())
        return has_changes or has_deletes

    def _encode_sync_payload(self, payload: Dict[str, Any]) -> str:
        """Encode a sync payload to compressed base64 string."""
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        b64 = base64.b64encode(zlib.compress(raw)).decode("ascii")
        return "DBSYNC LAZY " + b64

    async def sync_out(self) -> Optional[str]:
        if not self._has_pending_changes():
            return None

        changed = self._changed_dict()
        deleted = self._deleted_dict()
        changed_tables = set(changed.keys()) | set(deleted.keys())

        payload: Dict[str, Any] = {
            "format": SYNC_PAYLOAD_FORMAT_VERSION,
            "tables": {},
        }

        for table in changed_tables:
            changed_ids = changed.get(table, set())
            deleted_ids = deleted.get(table, set())

            # Collect changed documents
            docs: Dict[str, Any] = {}
            for id_ in changed_ids:
                doc = await self.get(table, id_)
                if doc is not None:
                    docs[id_] = doc

            payload["tables"][table] = {
                "docs": docs,
                "deleted_ids": sorted(list(deleted_ids)),
                "doc_versions": {
                    id_: int(self._tbl(table)["doc_versions"].get(id_, 0))
                    for id_ in (changed_ids | deleted_ids)
                },
            }

        return self._encode_sync_payload(payload)

    def _decode_sync_payload(self, data_b64: str) -> Dict[str, Any]:
        """Decode and validate a sync payload."""
        # Strip DBSYNC prefix if present
        if data_b64.startswith("DBSYNC LAZY "):
            data_b64 = data_b64[len("DBSYNC LAZY ") :]

        if not isinstance(data_b64, str):
            raise TypeError("sync_in expects a base64 string (without 'SYNC ' prefix)")

        try:
            zipped = base64.b64decode(data_b64.encode("ascii"))
            if len(zipped) > MAX_COMPRESSED_PAYLOAD_SIZE:
                raise ValueError("Compressed sync payload too large")

            # Decompress with size limit
            try:
                raw = zlib.decompress(zipped, max_length=MAX_RAW_PAYLOAD_SIZE)
            except TypeError:
                raw = zlib.decompress(zipped)
                if len(raw) > MAX_RAW_PAYLOAD_SIZE:
                    raise ValueError("raw payload too large")

            payload = json.loads(raw.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"Invalid sync payload: {e!s}")

        if int(payload.get("format", 0)) != SYNC_PAYLOAD_FORMAT_VERSION:
            raise ValueError("Unsupported sync payload format")

        return payload

    async def sync_in(
        self,
        data_b64: str,
    ) -> Dict[str, Any]:
        payload = self._decode_sync_payload(data_b64)

        applied_upserts = 0
        applied_deletes = 0
        conflicts: List[Tuple[str, str, str]] = []

        for table, entry in payload.get("tables", {}).items():
            # Process deletions
            for id_ in entry.get("deleted_ids", []):
                try:
                    await self._delete_local(table, id_)
                    applied_deletes += 1
                except Exception as e:
                    conflicts.append((table, id_, f"delete: {e!s}"))

            # Process upserts
            for id_, doc in entry.get("docs", {}).items():
                try:
                    await self._upsert_local(table, id_, doc, replace=True)
                    applied_upserts += 1
                except Exception as e:
                    conflicts.append((table, id_, f"upsert: {e!s}"))

        _reset_context_vars()

        return {
            "applied_upserts": applied_upserts,
            "applied_deletes": applied_deletes,
            "conflicts": conflicts,
        }

    async def snapshot_docs(
        self, table: str, ids: List[str]
    ) -> Dict[str, Optional[JSON]]:
        snap = {}
        for id_ in ids:
            snap[id_] = await self.get(table, id_)
        return snap

    async def apply_snapshot(
        self, table: str, snapshot: Dict[str, Optional[JSON]]
    ) -> None:
        for id_, doc in snapshot.items():
            if doc is None:
                await self._delete_local(table, id_)
        for id_, doc in snapshot.items():
            if doc is not None:
                d = dict(doc)
                d.pop("id", None)
                await self._upsert_local(table, id_, d, replace=True)

    async def make_sync_from_docs(
        self, tables_docs: Dict[str, Dict[str, Optional[JSON]]]
    ) -> str:
        payload = {
            "format": SYNC_PAYLOAD_FORMAT_VERSION,
            "tables": {},
        }

        for table, idmap in tables_docs.items():
            docs = {}
            deleted = []

            for id_, doc in idmap.items():
                if doc is None:
                    deleted.append(id_)
                else:
                    dd = dict(doc)
                    dd.setdefault("id", id_)
                    docs[id_] = dd

            payload["tables"][table] = {
                "docs": docs,
                "deleted_ids": sorted(deleted),
                "doc_versions": {
                    id_: int(self._tbl(table)["doc_versions"].get(id_, 0))
                    for id_ in set(list(docs.keys()) + deleted)
                },
            }

        return self._encode_sync_payload(payload)

    @_requires_cluster
    async def _do_ops(
        self,
        table: str,
        kind: Literal["delete", "upsert", "patch"],
        id_: str,
        doc: Optional[JSON],
        replace: bool,
        incoming_version: Optional[int],
    ) -> None:
        if kind not in ["delete", "upsert", "patch"]:
            raise ValueError(f"Unknown op {kind}")

        try:
            locks = _locks_ctx.get()
            if not id_ in locks:
                locks[id_] = await self.cluster.acquire_lock([id_])
                _locks_ctx.set(locks)
        except Exception as e:
            logger.critical(e)
            raise Exception(f"Could not acquire cluster lock for id {id_}")

        snapshots = _snapshots_ctx.get()
        if table not in snapshots:
            snapshots[table] = await self.snapshot_docs(table, ensure_list(id_))
            _snapshots_ctx.set(snapshots)

        if kind == "delete":
            await self._delete_local(table, id_)
        elif kind == "patch":
            await self._upsert_local(
                table,
                id_,
                doc,
                replace=False,
                incoming_version=incoming_version,
            )
        elif kind == "upsert":
            await self._upsert_local(
                table,
                id_,
                doc,
                replace=replace,
                incoming_version=incoming_version,
            )
