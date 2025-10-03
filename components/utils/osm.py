import json
from config.defaults import OSM_EMAIL, HOSTNAME
from .requests import async_request, sync_request


def coords_to_display_name(coords: str):
    from components.database.states import STATE

    if not isinstance(coords, str):
        raise ValueError(f"'coords' must be a string, but got {type(coords).__name__}.")

    try:
        lat_str, lon_str = coords.split(",")
        lat = float(lat_str)
        lon = float(lon_str)
        assert lon and lat
    except (ValueError, TypeError, AssertionError):
        raise ValueError(f"Invalid coordinate string: {coords}")

    if STATE.locations.get(coords):
        return STATE.locations[coords]

    else:
        try:
            status_code, response_text = sync_request(
                f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lon}&format=json&addressdetails=0&email={OSM_EMAIL}",
                "GET",
                headers={
                    "User-Agent": f"coords_to_display_name() - Thank you! - Contact: {OSM_EMAIL}",
                    "Referer": f"https://{HOSTNAME}",
                },
            )
            if status_code == 200:
                response_text = json.loads(response_text)
                STATE.locations[coords] = response_text["display_name"]
                return response_text["display_name"]
        except:
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
