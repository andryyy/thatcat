import asyncio
import os
import re

from components.models import Any, BaseModel, Literal, UUID
from copy import deepcopy


__all__ = [
    "merge_models",
    "merge_deep",
    "batch",
    "ensure_list",
    "to_unique_sorted_str_list",
    "is_path_within_cwd",
    "expire_key",
]


# Returns new model (based on original model) using
# deep merged data of both input models data
def merge_models(
    original: BaseModel,
    override: BaseModel,
    exclude_strategies: list = Literal[
        "exclude_original_none",
        "exclude_original_unset",
        "exclude_override_none",
        "exclude_override_unset",
    ],
) -> BaseModel:
    # Convert both models to dictionaries
    original_data = original.model_dump(
        mode="json",
        exclude_none=True if "exclude_original_none" in exclude_strategies else False,
        exclude_unset=True if "exclude_original_unset" in exclude_strategies else False,
    )
    override_data = override.model_dump(
        mode="json",
        exclude_none=True if "exclude_override_none" in exclude_strategies else False,
        exclude_unset=True if "exclude_override_unset" in exclude_strategies else False,
    )

    # Merge with override taking priority
    merged_data = merge_deep(original_data, override_data)

    # Return a revalidated model using the original model's class
    return original.__class__(**merged_data)


def deep_model_dump(value):
    if isinstance(value, BaseModel):
        return deep_model_dump(value.model_dump(mode="json"))
    elif isinstance(value, dict):
        return {k: deep_model_dump(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [deep_model_dump(item) for item in value]
    elif isinstance(value, tuple):
        return tuple(deep_model_dump(item) for item in value)
    elif isinstance(value, set):
        return {deep_model_dump(item) for item in value}
    else:
        return value


def merge_deep(original_data: dict, override_data: dict):
    result = deepcopy(original_data)

    def _recursive_merge(_original_data, _override_data):
        for key in _override_data:
            if (
                key in _original_data
                and isinstance(_original_data[key], dict)
                and isinstance(_override_data[key], dict)
            ):
                _recursive_merge(_original_data[key], _override_data[key])
            else:
                _original_data[key] = _override_data[key]

    _recursive_merge(result, override_data)
    return result


def batch(l: list, n: int):
    _l = len(l)
    for ndx in range(0, _l, n):
        yield l[ndx : min(ndx + n, _l)]


def chunk_string(s, size=1_000_000):
    return [s[i : i + size] for i in range(0, len(s), size)]


def ensure_list(a: Any | list[Any] | None) -> list:
    if a:
        if not isinstance(a, list):
            return [a]
        return a
    return []


def ensure_unique_list(a: list[Any]) -> list:
    if isinstance(a, str):
        return [a]
    seen = set()
    return [x for x in a if x and not (x in seen or seen.add(x))]


def to_unique_sorted_str_list(l: list[Any]) -> list:
    _l = [str(x) for x in set(l) if x]
    return sorted(_l)


def is_path_within_cwd(path):
    requested_path = os.path.abspath(path)
    return requested_path.startswith(os.path.abspath("."))


async def expire_key(in_dict: dict, dict_key: int | str, wait_time: float | int):
    try:
        await asyncio.sleep(wait_time)
        in_dict.pop(dict_key, None)
    except asyncio.CancelledError:
        pass
