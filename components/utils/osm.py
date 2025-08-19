import json
from config.defaults import OSM_EMAIL, HOSTNAME
from components.database import STATE
from components.models.coords import Literal, Location, validate_call


@validate_call
async def coords_to_display_name(coords: str):
    from components.utils.requests import async_request

    location = Location.from_coords(coords)

    if STATE.locations.get(location.coords):
        return STATE.locations[location.coords]
    else:
        try:
            status_code, response_text = await async_request(
                f"https://nominatim.openstreetmap.org/reverse?lat={location.lat}&lon={location.lon}&format=json&addressdetails=0&email={OSM_EMAIL}",
                "GET",
                headers={
                    "User-Agent": f"coords_to_display_name() - Thank you! - Contact: {OSM_EMAIL}",
                    "Referer": f"https://{HOSTNAME}",
                },
            )
            if status_code == 200:
                response_text = json.loads(response_text)
                STATE.locations[location.coords] = response_text["display_name"]
                return response_text["display_name"]
        except:
            return None


@validate_call
async def display_name_to_location(q: str):
    from components.utils.requests import async_request

    if q in STATE.locations:
        return STATE.locations[q]
    else:
        try:
            location = None
            status_code, response_text = await async_request(
                f"https://nominatim.openstreetmap.org/search?q={q}&limit=1&format=json&email={OSM_EMAIL}",
                "GET",
                headers={
                    "User-Agent": f"display_name_to_location() - Thank you! - Contact: {OSM_EMAIL}",
                    "Referer": f"https://{HOSTNAME}",
                },
            )
            if status_code == 200:
                location = None
                response_text = json.loads(response_text)

                if response_text:
                    result = response_text[0]
                    location = Location(
                        lat=result["lat"],
                        lon=result["lon"],
                        display_name=result["display_name"],
                    )

                STATE.locations[q] = location

        finally:
            return location
