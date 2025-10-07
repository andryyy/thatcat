import json
import re

from uuid import UUID


def email_validator(email: str) -> bool:
    ATOM_CHAR = r"[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]"
    DOT_ATOM = rf"(?:{ATOM_CHAR}+)(?:\.{ATOM_CHAR}+)*"
    QUOTED_STRING = r'"(?:\\[\x00-\x7f]|[^"\\])*"'
    LOCAL_PART = rf"(?:{DOT_ATOM}|{QUOTED_STRING})"
    DOMAIN_LABEL = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    FQDN = rf"(?:{DOMAIN_LABEL}\.)+([A-Za-z]{{2,}})"
    IPv4_LITERAL = r"\[(?:\d{1,3}\.){3}\d{1,3}\]"
    IPv6_LITERAL = r"\[IPv6:[0-9A-Fa-f:.]+\]"
    DOMAIN_PART = rf"(?:{FQDN}|{IPv4_LITERAL}|{IPv6_LITERAL})"
    EMAIL_REGEX = re.compile(rf"^{LOCAL_PART}@{DOMAIN_PART}$")
    return EMAIL_REGEX.fullmatch(email) is not None


def validate_uuid_str(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"Value must be string, got {type(value).__name__}")
    try:
        return str(UUID(value))
    except Exception as exc:
        raise ValueError(f"Invalid UUID like string: {value!r}")


def to_location(val: dict | object) -> "Location":
    from .coords import Location
    from components.utils.osm import CoordsResolver

    if isinstance(val, Location):
        if val.lon == 0.0 or val.lat == 0.0:
            return None
        if not val.display_name:
            try:
                val.display_name = CoordsResolver(val.coords).resolve()
            except:
                val.display_name = ""
        return val
    try:
        return to_location(Location(**val))
    except Exception as exc:
        raise ValueError(f"Cannot convert '{val!r}' to Location") from exc


def to_int(val: int | str | None) -> int:
    if isinstance(val, int):
        return val
    try:
        return int(val or 0)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Cannot convert '{val!r}' to int")


def to_str(val: int | str | None) -> str:
    try:
        return str(val or "")
    except:
        raise ValueError(f"Cannot convert '{val!r}' to str")


def to_bool(val: bool | str) -> bool:
    if isinstance(val, bool):
        return val
    lowered = str(val).lower()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    raise ValueError(f"Invalid boolean value: {val!r}")


def to_asset_from_str(json_str: str) -> list["Asset"]:
    from .assets import Asset

    try:
        raw_items = json.loads(json_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid assets JSON: {exc}")

    if not isinstance(raw_items, list):
        raise TypeError("Assets JSON must represent a list")

    assets = []
    for item in raw_items:
        if not isinstance(item, dict):
            raise TypeError("Each asset entry must be a JSON object")
        assets.append(Asset(**item))
    return assets
