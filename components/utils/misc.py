import os
import sys
from typing import Any

__all__ = [
    "batch",
    "ensure_list",
    "unique_list",
    "to_unique_sorted_str_list",
    "is_path_within_cwd",
]


def batch(lst: list, n: int):
    _l = len(lst)
    for ndx in range(0, _l, n):
        yield lst[ndx : min(ndx + n, _l)]


def ensure_list(x: Any) -> list:
    if isinstance(x, (list, tuple, set)):
        return list(x)
    if isinstance(x, (str, dict)):
        return [x]
    return []


def unique_list(lst: list[Any] | set[Any]) -> list:
    if isinstance(lst, list):
        return list(dict.fromkeys(lst))
    elif isinstance(lst, set):
        return list(lst)
    raise TypeError("Input is not a list or set")


def to_unique_sorted_str_list(lst: list[Any]) -> list:
    _lst = [str(x) for x in set(lst) if x]
    return sorted(_lst)


def is_path_within_cwd(path):
    requested_path = os.path.abspath(path)
    main_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    return requested_path.startswith(main_dir)
