from __future__ import annotations

from typing import Any, List

from components.logs import logger
from components.utils.misc import ensure_list


def get_all(doc: Any, path: str) -> List[Any]:
    """Extract all values at a given path in a document.

    Handles nested paths with dot notation and lists.
    For example, "credentials.id" will extract all credential IDs from a document.

    Args:
        doc: The document to search
        path: Dot-separated path (e.g., "user.name" or "credentials.id")

    Returns:
        List of all values found at the path
    """
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


def merge_dict(dst: Any, src: Any) -> Any:
    """Deep merge two dictionaries.

    Recursively merges src into dst. For nested dictionaries, merges recursively.
    For other types, src overwrites dst.

    Args:
        dst: Destination dictionary
        src: Source dictionary to merge

    Returns:
        Merged result
    """
    if isinstance(dst, dict) and isinstance(src, dict):
        out = dict()
        for k in set(dst.keys()) | set(src.keys()):
            if k in src:
                if k in dst:
                    out[k] = merge_dict(dst[k], src[k])
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


def match_clause(row: dict, clause: dict) -> bool:
    """Check if a row/doc matches a filter clause.

    For each field in the clause:
    - Get all values at that path in the document (handles nested paths)
    - Check if any clause value matches any document value (OR within field)
    - All fields must match (AND across fields)
    """
    for k, v in (clause or {}).items():
        options = ensure_list(v)
        dvals = get_all(row, k)
        if not any(opt in dvals for opt in options):
            return False
    return True


def filter_rows(
    rows_in: list[dict],
    where: dict | None,
    any_of: list[dict] | None,
    q: str | None,
) -> list[dict]:
    """Filter rows based on where, any_of, and q parameters."""
    if where is None and any_of is None and not q:
        return rows_in

    out = []
    for r in rows_in:
        ok = True
        if where:
            ok = match_clause(r, where)
        if ok and any_of:
            for c in any_of:
                if where:
                    for conflict in where.keys() & c.keys():
                        c.pop(conflict, None)
                        logger.warning(
                            f"Overlapping key {conflict!r} in 'where' and 'any_of' clause. "
                            + "Key will be removed from 'any_of' clause"
                        )
                if match_clause(r, c):
                    break
            else:
                ok = False
        if ok and q:
            needle = q.lower()
            ok = any(
                (isinstance(v, str) and needle in v.lower())
                or (not isinstance(v, (dict, list)) and needle in str(v).lower())
                for v in r.values()
            )
        if ok:
            out.append(r)
    return out


def type_rank(v: Any) -> int:
    """Return a sort rank for a value's type."""
    if v is None:
        return 5
    if isinstance(v, bool):
        return 2
    if isinstance(v, (int, float)):
        return 0
    if isinstance(v, str):
        return 1
    return 3


def normalize_sort_value(v: Any) -> Any:
    """Normalize a value for sorting."""
    if v is None:
        return ""
    if isinstance(v, str):
        return v.lower()
    return v


def create_sort_key(sort_attr: str | int, sort_reverse: bool):
    """Create a sort key function for sorting rows."""

    def key_func(row: dict):
        v = row.get(sort_attr, None)
        missing = v is None
        missing_key = 1 if missing else 0
        if sort_reverse:
            missing_key = 1 - missing_key
        return (missing_key, type_rank(v), normalize_sort_value(v))

    return key_func


def paginate_rows(
    rows: list[dict],
    page: int,
    page_size: int,
    sort_attr: str | int,
    sort_reverse: bool,
) -> dict:
    """Apply pagination to rows and return pagination metadata."""
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
