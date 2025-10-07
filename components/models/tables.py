from config import defaults
from components.utils.misc import ensure_list
from components.models.helpers import *
from dataclasses import dataclass, field
from config.defaults import TABLE_PAGE_SIZE


@dataclass
class TableSearch:
    q: str = ""
    page: str | int = 1
    page_size: str | int = defaults.TABLE_PAGE_SIZE
    sorting: str | tuple = ("id", True)
    filters: str | list | dict = field(default_factory=dict)

    def __post_init__(self):
        self.q = to_str(self.q)
        self.page = to_int(self.page) or 1
        self.page_size = to_int(self.page_size) or defaults.TABLE_PAGE_SIZE
        self.sorting = self._split_sorting(self.sorting)
        self.filters = self._filters_formatter(self.filters)

    @staticmethod
    def _split_sorting(v: str | tuple) -> tuple:
        if isinstance(v, str):
            parts = v.split(":")
            if len(parts) == 2:
                sort_attr, direction = parts
                sort_reverse = True if direction == "desc" else False
            else:
                sort_attr, sort_reverse = "id", True
            return (sort_attr, sort_reverse)

        return v

    @staticmethod
    def _filters_formatter(v: str | list | dict) -> dict:
        if isinstance(v, dict):
            return v

        filters = {}
        for f in ensure_list(v):
            key_name, key_value = f.split(":", 1)
            if key_name not in filters:
                filters[key_name] = key_value
            else:
                if isinstance(filters[key_name], list):
                    filters[key_name].append(key_value)
                else:
                    filters[key_name] = [filters[key_name], key_value]
        return filters
