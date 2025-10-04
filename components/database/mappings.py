from dataclasses import dataclass, field


_default_list_row = ["id", "created", "updated", "doc_version"]


@dataclass(frozen=True)
class Mappings:
    LIST_ROW_FIELDS: dict = field(
        default_factory=lambda: {
            "users": _default_list_row + ["login"],
            "projects": _default_list_row + ["name", "assigned_users"],
            "cars": _default_list_row + ["vin", "assigned_users", "assigned_project"],
            "processings": _default_list_row + ["assigned_user"],
        }
    )


mappings = Mappings()
