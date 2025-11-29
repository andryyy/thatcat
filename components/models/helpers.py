import json
import re
from components.utils.misc import ensure_list
from uuid import UUID
from typing import Any

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
HEX_COLOR_PATTERN_STANDARD = re.compile(r"^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$")


def hex_color_validator(color_string: str) -> bool:
    return bool(HEX_COLOR_PATTERN_STANDARD.match(color_string))


def email_validator(email: str) -> bool:
    return EMAIL_REGEX.fullmatch(email) is not None


def validate_uuid_str(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"Value must be string, got {type(value).__name__}")
    try:
        return str(UUID(value))
    except Exception:
        raise ValueError(f"Invalid UUID like string: {value!r}")


def to_location(val: dict | object) -> "Location":  # noqa: F821
    from .coords import Location
    from components.utils.osm import CoordsResolver

    if isinstance(val, Location):
        if val.lon == 0.0 or val.lat == 0.0:
            return None
        if not val.display_name:
            try:
                val.display_name = CoordsResolver(val.coords).resolve()
            except Exception:
                val.display_name = ""
        return val
    try:
        return to_location(Location(**val))
    except Exception as exc:
        raise ValueError(f"Cannot convert '{val!r}' to Location") from exc


def to_float(val: int | str | None) -> int:
    if isinstance(val, float):
        return val
    try:
        return float(val or 0.0)
    except (ValueError, TypeError):
        raise ValueError(f"Cannot convert '{val!r}' to float")


def to_int(val: int | str | None) -> int:
    if isinstance(val, int):
        return val
    try:
        return int(val or 0)
    except (ValueError, TypeError):
        raise ValueError(f"Cannot convert '{val!r}' to int")


def to_str(val: int | str | None) -> str:
    try:
        return str(val or "")
    except Exception:
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


def to_assets(data: Any) -> list["Asset"]:  # noqa: F821
    from .assets import Asset

    assets = []
    for item in ensure_list(data):
        if isinstance(item, Asset):
            assets.append(item)
        elif isinstance(item, dict):
            assets.append(Asset(**item))
        elif isinstance(item, str):
            try:
                item = json.loads(item)
                assets.extend(to_assets(item))
            except json.JSONDecodeError:
                raise ValueError("assets", f"Invalid asset JSON: {item}")
        else:
            raise TypeError(
                "assets",
                "All items in 'assets' must be of type Asset, dict or JSON string",
            )

    return assets


def to_car_markers(data: Any) -> list["CarMarker"]:  # noqa: F821
    from .markers import CarMarker

    car_markers = []
    for item in ensure_list(data):
        if isinstance(item, CarMarker):
            car_markers.append(item)
        elif isinstance(item, dict):
            car_markers.append(CarMarker(**item))
        elif isinstance(item, str):
            try:
                item = json.loads(item)
                car_markers.extend(to_car_markers(item))
            except json.JSONDecodeError:
                raise ValueError("car_markers", f"Invalid car_markers JSON: {item}")
        else:
            raise TypeError(
                "car_markers",
                "All items in 'car_markers' must be of type CarMarker, dict or JSON string",
            )

    return car_markers
