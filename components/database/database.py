from __future__ import annotations

import asyncio, json, zlib, base64, contextvars
from pathlib import Path
from typing import Dict, Any, Optional, List, Set, Tuple
from collections import OrderedDict
from contextlib import asynccontextmanager
from components.logs import logger
from components.utils import ensure_list


try:
    import msgpack
except Exception:
    msgpack = None

JSON = Dict[str, Any]

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
    def __init__(self, kind: str = "msgpack"):
        kind = (kind or "msgpack").lower()
        if kind not in {"json", "msgpack"}:
            raise ValueError("codec must be 'json' or 'msgpack'")
        if kind == "msgpack" and msgpack is None:
            raise RuntimeError(
                "MessagePack codec requested but 'msgpack' is not installed. pip install msgpack"
            )
        self.kind = kind

    def dumps(self, obj: dict) -> bytes:
        if self.kind == "msgpack":
            return msgpack.dumps(obj, use_bin_type=True)
        elif self.kind == "json":
            return json.dumps(obj, ensure_ascii=False).encode("utf-8")

    def loads(self, data: bytes) -> dict:
        if self.kind == "msgpack":
            return msgpack.loads(data, raw=False)
        elif self.kind == "json":
            return json.loads(data.decode("utf-8"))


def _get_all(doc: Any, path: str) -> List[Any]:
    parts = path.split(".")

    def walk(cur, idx):
        if idx == len(parts):
            if isinstance(cur, list):
                return [it for it in cur]
            return [cur]

        key = parts[idx]
        if isinstance(cur, dict):
            if key in cur:
                return walk(cur[key], idx + 1)
            return []

        if isinstance(cur, list):
            out = []
            for it in cur:
                out.extend(walk(it, idx))
            return out

        return []

    return walk(doc, 0)


def _get_first(doc: Any, path: str, default=None):
    vals = _get_all(doc, path)
    return vals[0] if vals else default


def _deep_merge(dst: Any, src: Any) -> Any:
    if isinstance(dst, dict) and isinstance(src, dict):
        out = dict()
        for k in set(dst.keys()) | set(src.keys()):
            if k in src:
                if k in dst:
                    out[k] = _deep_merge(dst[k], src[k])
                else:
                    out[k] = src[k]
            else:
                v = dst[k]
                if isinstance(v, dict):
                    out[k] = dict(v)
                elif isinstance(v, list):
                    out[k] = list(v)
                else:
                    out[k] = v
        return out
    return src


class _LRU:
    def __init__(self, max_entries: int = 2048):
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
        self._open = False
        self._manifest: JSON = {}
        self._peer_manifests: Dict[str, JSON] = {}
        self._indexes: Dict[str, Dict[str, Dict[Any, Set[str]]]] = {}
        self._lists: Dict[str, Dict[str, Any]] = {}
        self._cache = _LRU(max_entries=2048)
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

    def _validate_id(self, id_: str):
        if not id_:
            raise ValueError("id must be non‑empty")
        if not isinstance(id_, str):
            raise TypeError("id must be str")
        if "/" in id_ or "\\" in id_ or id_.startswith("."):
            raise ValueError(f"Invalid document id {id_!r}")

    def _resolve_doc_path(self, table: str, id_: str) -> Path:
        self._validate_id(id_)
        tdir = (self.base / table).resolve()
        p = (self.base / table / id_).resolve()
        if p.parent != tdir:
            raise ValueError(f"Invalid document id {id_!r}")
        return p

    def _lock_for(self, table: str, id_: str) -> asyncio.Lock:
        key = (table, id_)
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def do_rollback(self, cluster_peers: list = []):
        try:
            snapshots = _snapshots_ctx.get()
            if not snapshots:
                logger.debug("Nothing to rollback")
                return
            for table in snapshots:
                await self.apply_snapshot(table, snapshots[table])
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

    async def __aenter__(self):
        _reset_context_vars()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        sync_str = await self.sync_out()
        if sync_str and not exc:
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

        elif exc:
            logger.error(f"Rolling back local changes due to error: {exc}.")
            await asyncio.shield(self.do_rollback())

        async with self._lock_for("aexit", "1"):
            locks = _locks_ctx.get()
            for doc_id, lock_id in locks.items():
                await asyncio.shield(self.cluster.release(lock_id, [doc_id]))
            self.main_path.write_text(json.dumps(self._manifest, indent=2))

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

    async def ids(self, table: str) -> List[str]:
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
        if table not in self._indexes:
            return
        for f, mapping in self._indexes[table].items():
            if old_doc is not None:
                for v in _get_all(old_doc, f):
                    try:
                        s = mapping.get(v)
                    except TypeError:
                        logger.warning(f"Type Error for {v}")
                        continue
                    if s:
                        s.discard(id_)
                        if not s:
                            mapping.pop(v, None)
            if new_doc is not None:
                for v in _get_all(new_doc, f):
                    try:
                        bucket = mapping.setdefault(v, set())
                    except TypeError:
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
                else (_deep_merge(old_doc, doc) if old_doc is not None else doc)
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

            if table in self._lists:
                fields = self._lists[table]["fields"]
                row = {"id": id_}
                for f in fields:
                    row[f] = id_ if f == "id" else _get_first(cached_doc, f, None)
                self._lists[table]["rows"][id_] = row

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

            if table in self._lists:
                self._lists[table]["rows"].pop(id_, None)

            self._locks.pop((table, id_), None)

    async def build_index(self, table: str, fields: List[str]) -> None:
        idxs = self._indexes.setdefault(table, {})
        for f in fields:
            idxs[f] = {}
        for id_ in await self.ids(table):
            doc = await self.get(table, id_)
            if not doc:
                continue
            for f in fields:
                for v in _get_all(doc, f):
                    try:
                        bucket = idxs[f].setdefault(v, set())
                    except TypeError:
                        continue
                    bucket.add(id_)

    async def search(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        *,
        any_of: Optional[List[Dict[str, Any]]] = None,
        matched_only: bool = False,
        return_fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> List[JSON]:
        """
        Indexed search with logical AND/OR and optional projection.

        Semantics:
          - where: AND across fields; within-field you can pass a list for OR (e.g., {"status": ["open","pending"]})
          - any_of: OR across clauses (each clause uses the same semantics as 'where')
          - matched_only: return only matched fields (plus 'id'), values are subsets at dotted paths
          - return_fields: additional projected fields (supports dotted paths)
        """
        where = where or {}
        clauses = any_of or [where]

        def _vals(x):
            if isinstance(x, (list, tuple, set)):
                return list(x)
            return [x]

        def _project_value(doc: JSON, path: str):
            vals = _get_all(doc, path)
            if len(vals) == 1:
                return vals[0]
            return vals

        results: Set[str] = set()

        for clause in clauses:
            idxs = self._indexes.get(table, {})
            candidate_ids: Optional[Set[str]] = None
            for f, val in clause.items():
                values = _vals(val)
                if f in idxs:
                    field_results = set()
                    for v in values:
                        field_results |= idxs[f].get(v, set())
                    if candidate_ids is None:
                        candidate_ids = field_results
                    else:
                        candidate_ids &= field_results
                else:
                    logger.warning(
                        f"Search on unindexed field '{f}' in table '{table}' may be slow"
                    )
            if candidate_ids is None:
                candidate_ids = set(await self.ids(table))
            matched = set()
            for id_ in candidate_ids:
                doc = await self.get(table, id_)
                if not doc:
                    continue
                ok = True
                for f, val in clause.items():
                    values = _vals(val)
                    dvals = _get_all(doc, f)
                    if not any(v in dvals for v in values):
                        ok = False
                        break
                if ok:
                    matched.add(id_)
            results |= matched
            if limit is not None and len(results) >= limit:
                break

        out: List[JSON] = []
        ordered = sorted(results)
        if limit is not None:
            ordered = ordered[:limit]
        for id_ in ordered:
            doc = await self.get(table, id_)
            if not doc:
                continue

            if matched_only:
                slim: Dict[str, Any] = {"id": id_}
                fieldset: Set[str] = set()
                for c in clauses:
                    fieldset.update(c.keys())
                for f in fieldset:
                    wanted = set()
                    for c in clauses:
                        if f in c:
                            wanted |= set(_vals(c[f]))
                    subset = [v for v in _get_all(doc, f) if v in wanted]
                    if subset:
                        slim[f] = subset if len(subset) > 1 else subset[0]
                if return_fields:
                    for f in return_fields:
                        if f == "id":
                            slim["id"] = id_
                        else:
                            slim[f] = _project_value(doc, f)
                out.append(slim)
            else:
                if return_fields:
                    proj: Dict[str, Any] = {"id": id_}
                    for f in return_fields:
                        if f == "id":
                            proj["id"] = id_
                        else:
                            proj[f] = _project_value(doc, f)
                    out.append(proj)
                else:
                    out.append(doc)
        return out

    async def define_list_view(self, table: str, fields: List[str]) -> None:
        rows = {}
        for id_ in await self.ids(table):
            doc = await self.get(table, id_)
            if not doc:
                continue
            row = {"id": id_}
            for f in fields:
                row[f] = _get_first(doc, f, None)
            rows[id_] = row
        self._lists[table] = {"fields": list(fields), "rows": rows}

    async def list_rows(
        self,
        table: str,
        *,
        page: int = 1,
        page_size: int = 50,  # -1 => return all rows
        sort_attr: str | int = "id",  # -1 => no sort
        sort_reverse: bool = False,
        where: dict | None = None,
        any_of: list[dict] | None = None,
        q: str | None = None,
        prefer_indexed: bool = False,
    ):
        """
        Filtering:
          - where: AND across fields; within a field pass a list for OR (e.g., {"status": ["open","pending"]})
          - any_of: OR across clauses, each clause has same semantics as 'where'
          - q: case-insensitive substring applied to stringified row values
          - prefer_indexed: if True, use search() to prefilter IDs via indexes, then render rows

        Notes:
          - Filtering happens on the *row projection* (first value at each path). For deep/list-aware matches use search().
          - If no list view was defined, falls back to rows = [{"id": ...}] so callers won't crash.
        """

        lst = self._lists.get(table)
        if not lst or "rows" not in lst:
            ids = await self.ids(table)
            rows = [{"id": i} for i in ids]
        else:
            rows = list(lst["rows"].values())

        if prefer_indexed and (where or any_of):
            hits = await self.search(
                table, where=where, any_of=any_of, return_fields=["id"]
            )
            allow_ids = {h["id"] for h in hits}
            rows = [r for r in rows if r.get("id") in allow_ids]

        def _vals(x):
            if isinstance(x, (list, tuple, set)):
                return list(x)
            return [x]

        def _match_clause(row: dict, clause: dict) -> bool:
            for k, v in (clause or {}).items():
                rv = row.get(k, None)
                options = _vals(v)
                if not any(rv == opt for opt in options):
                    return False
            return True

        def _filter_rows(rows_in: list[dict]) -> list[dict]:
            if where is None and any_of is None and not q:
                return rows_in
            out = []
            for r in rows_in:
                ok = True
                if any_of:
                    ok = any(_match_clause(r, c) for c in any_of)
                elif where:
                    ok = _match_clause(r, where)
                if ok and q:
                    needle = q.lower()
                    ok = any(
                        (isinstance(v, str) and needle in v.lower())
                        or (
                            not isinstance(v, (dict, list)) and needle in str(v).lower()
                        )
                        for v in r.values()
                    )
                if ok:
                    out.append(r)
            return out

        rows = _filter_rows(rows)

        if sort_attr != -1:

            def _type_rank(v):
                if v is None:
                    return 5
                if isinstance(v, bool):
                    return 2
                if isinstance(v, (int, float)):
                    return 0
                if isinstance(v, str):
                    return 1
                return 3

            def _norm(v):
                if v is None:
                    return ""
                if isinstance(v, str):
                    return v.lower()
                return v

            def _key(row):
                v = row.get(sort_attr, None)
                missing = v is None
                missing_key = 1 if missing else 0
                if sort_reverse:
                    missing_key = 1 - missing_key
                return (missing_key, _type_rank(v), _norm(v))

            rows.sort(key=_key, reverse=sort_reverse)

        total = len(rows)
        if page_size == -1:
            items = rows
            total_pages = 1
            page = 1
        else:
            if page_size < 1:
                page_size = 1
            total_pages = max(1, (total + page_size - 1) // page_size)
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages
            start = (page - 1) * page_size
            end = start + page_size
            items = rows[start:end]

        return {
            "items": items,
            "total": total,
            "page": page,
            "page_size": page_size,
            "sort_attr": sort_attr,
            "sort_reverse": sort_reverse,
            "total_pages": total_pages,
            "has_prev": (page > 1) and (page_size != -1),
            "has_next": (page < total_pages) and (page_size != -1),
        }

    async def sync_out(self) -> Optional[str]:
        changed = self._changed_dict()
        deleted = self._deleted_dict()
        has_changes = any(len(s) > 0 for s in changed.values())
        has_deletes = any(len(s) > 0 for s in deleted.values())
        if not (has_changes or has_deletes):
            return None

        changed_tables = set(changed.keys()) | set(deleted.keys())

        payload: Dict[str, Any] = {
            "format": 2,
            "tables": {},
        }
        for table in changed_tables:
            changed_ids = changed.get(table, set())
            deleted_ids = deleted.get(table, set())
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
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        b64 = base64.b64encode(zlib.compress(raw)).decode("ascii")
        return "DBSYNC LAZY " + b64

    async def sync_in(
        self,
        data_b64: str,
    ) -> Dict[str, Any]:
        if data_b64.startswith("DBSYNC LAZY "):
            data_b64 = data_b64[5:]

        if not isinstance(data_b64, str):
            raise TypeError("sync_in expects a base64 string (without 'SYNC ' prefix)")

        try:
            zipped = base64.b64decode(data_b64.encode("ascii"))
            if len(zipped) > 32 * 1024 * 1024:
                raise ValueError("Compressed sync payload too large")
            try:
                raw = zlib.decompress(zipped, max_length=128 * 1024 * 1024)
            except TypeError:
                raw = zlib.decompress(zipped)
                if len(raw) > 128 * 1024 * 1024:
                    raise ValueError("raw payload too large")
            payload = json.loads(raw.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"Invalid sync payload: {e!s}")

        if int(payload.get("format", 0)) != 2:
            raise ValueError("Unsupported sync payload format")

        applied_upserts = 0
        applied_deletes = 0
        conflicts: List[Tuple[str, str, str]] = []

        for table, entry in payload.get("tables", {}).items():
            for id_ in entry.get("deleted_ids", []):
                try:
                    await self._delete_local(table, id_)
                    applied_deletes += 1
                except Exception as e:
                    conflicts.append((table, id_, f"delete: {e!s}"))

            for id_, doc in entry.get("docs", {}).items():
                incoming_version = entry.get("doc_versions", {}).get(id_)
                try:
                    await self._upsert_local(
                        table,
                        id_,
                        doc,
                        replace=True,
                        # incoming_version=incoming_version, # set to sync_in provided ver
                    )
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
            "format": 2,
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
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        b64 = base64.b64encode(zlib.compress(raw)).decode("ascii")
        return "DBSYNC LAZY " + b64

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
