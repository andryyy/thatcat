import asyncio
import os

from typing import Any

__all__ = [
    "batch",
    "ensure_list",
    "unique_list",
    "to_unique_sorted_str_list",
    "is_path_within_cwd",
]


def batch(l: list, n: int):
    _l = len(l)
    for ndx in range(0, _l, n):
        yield l[ndx : min(ndx + n, _l)]


def ensure_list(a: Any | list[Any] | None) -> list:
    if a:
        if not isinstance(a, list):
            return [a]
        return a
    return []


def unique_list(l: list[Any]) -> list:
    if isinstance(l, list):
        return list(dict.fromkeys(l))
    raise ValueError("Input is not a list")


def to_unique_sorted_str_list(l: list[Any]) -> list:
    _l = [str(x) for x in set(l) if x]
    return sorted(_l)


def is_path_within_cwd(path):
    requested_path = os.path.abspath(path)
    return requested_path.startswith(os.path.abspath("."))
