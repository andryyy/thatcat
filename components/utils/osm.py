import json

from .requests import async_request, sync_request
from components.logs import logger
from config.defaults import HOSTNAME, OSM_EMAIL
from components.database.states import STATE


class CoordsResolver:
    def __init__(self, coords: str):
        if not isinstance(coords, str):
            raise ValueError(
                f"'coords' must be a string, but got {type(coords).__name__}."
            )

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


async def display_name_to_location(q: str) -> dict:
    from components.database.states import STATE

    if not isinstance(q, str):
        raise ValueError(f"'q' must be a string, but got {type(q).__name__}.")

    if q in STATE.locations:
        return STATE.locations[q]
    else:
        try:
            result = {}
            status_code, response_text = await async_request(
                f"https://nominatim.openstreetmap.org/search?q={q}&limit=1&format=json&email={OSM_EMAIL}",
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
                    STATE.locations[q] = result

        finally:
            return result
