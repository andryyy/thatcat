import json

from .requests import async_request, sync_request
from components.database.states import STATE
from components.logs import logger
from config.defaults import HOSTNAME, OSM_EMAIL
from urllib.parse import urlencode, quote_plus


class CoordsResolver:
    def __init__(self, coords: str):
        if not isinstance(coords, str):
            raise ValueError(f"'coords' must be a string, got {type(coords).__name__}.")

        try:
            lat_str, lon_str = coords.split(",")
            self.lat = float(lat_str)
            self.lon = float(lon_str)
            self.coords = coords
            assert self.lon and self.lat
        except (ValueError, TypeError, AssertionError):
            raise ValueError(f"Invalid coordinate string: {coords}")

        self.display_name = None
        if STATE.locations.get(coords):
            self.display_name = STATE.locations[coords]

    def resolve(self, force: bool = False):
        if self.display_name and not force:
            return self.display_name
        try:
            status_code, response_text = sync_request(
                f"https://nominatim.openstreetmap.org/reverse?lat={self.lat}&lon={self.lon}&format=json&addressdetails=0&email={OSM_EMAIL}",
                "GET",
                headers={
                    "User-Agent": f"coords_to_display_name() - Thank you! - Contact: {OSM_EMAIL}",
                    "Referer": f"https://{HOSTNAME}",
                },
            )
            if status_code == 200:
                response_text = json.loads(response_text)
                STATE.locations[self.coords] = response_text["display_name"]
                return response_text["display_name"]
        except Exception as e:
            logger.error(f"Cannot resolve coords: {e}")
            return None

    async def aresolve(self, force: bool = False):
        if self.display_name and not force:
            return self.display_name

        try:
            status_code, response_text = await async_request(
                f"https://nominatim.openstreetmap.org/reverse?lat={self.lat}&lon={self.lon}&format=json&addressdetails=0&email={OSM_EMAIL}",
                "GET",
                headers={
                    "User-Agent": f"coords_to_display_name() - Thank you! - Contact: {OSM_EMAIL}",
                    "Referer": f"https://{HOSTNAME}",
                },
            )
            if status_code == 200:
                response_text = json.loads(response_text)
                STATE.locations[self.coords] = response_text["display_name"]
                return response_text["display_name"]
        except Exception as e:
            logger.error(f"Cannot resolve coords: {e}")
            return None


async def display_name_to_location(q: str | dict) -> dict:
    from components.database.states import STATE

    if not isinstance(q, (str, dict)):
        raise ValueError(f"'q' must be a string or dict, got {type(q).__name__}.")

    if not q:
        raise ValueError("'q' must not be empty")

    data = {
        "email": OSM_EMAIL,
        "format": "json",
        "limit": 1,
    }

    if isinstance(q, dict):
        for attr in q:
            if q[attr] is not None and attr in ["country", "city", "street"]:
                clean_text = q[attr].strip()
                if clean_text:
                    data[attr] = clean_text
    else:
        data["q"] = q.strip()

    query_string = urlencode(data, quote_via=quote_plus)

    if query_string in STATE.locations:
        return STATE.locations[query_string]
    else:
        result = {}
        status_code, response_text = await async_request(
            f"https://nominatim.openstreetmap.org/search?{query_string}",
            "GET",
            headers={
                "User-Agent": f"display_name_to_location() - Thank you! - Contact: {OSM_EMAIL}",
                "Referer": f"https://{HOSTNAME}",
            },
        )
        if status_code == 200:
            response_text = json.loads(response_text)

            if response_text:
                result = response_text[0]
                STATE.locations[query_string] = result

            return result

        return result
