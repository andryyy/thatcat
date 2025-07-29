import os

from components.models import *
from components.utils.datetimes import ntime_utc_now, utc_now_as_str


class Location(BaseModel):
    lat: float
    lon: float
    display_name: str

    @computed_field
    @property
    def coords(self) -> str:
        return ",".join([str(self.lat), str(self.lon)])

    @computed_field
    @property
    def _is_valid(self) -> bool:
        return all(bool(l) for l in [self.lat, self.lon])

    @classmethod
    def from_coords(self, coords: str) -> "Coords":
        try:
            lat, lon = coords.split(",")
            return self(lat=lat, lon=lon, display_name="")
        except:
            raise PydanticCustomError(
                "coords",
                "Ungültige Koordinaten",
                dict(),
            )
