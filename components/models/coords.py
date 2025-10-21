from dataclasses import dataclass
from components.models.helpers import to_int, to_str


@dataclass
class Location:
    lat: float | str
    lon: float | str
    display_name: str | None = None
    radius: int | str = 100
    zoom: int | str = 12

    @property
    def coords(self) -> str:
        return f"{self.lat},{self.lon}"

    @classmethod
    def from_coords(cls, coords: str) -> "Location":
        try:
            lat_str, lon_str = coords.split(",")
            return cls(lat=float(lat_str), lon=float(lon_str), display_name="")
        except (ValueError, TypeError):
            raise ValueError(f"Invalid coordinate string: {coords}")

    def __post_init__(self):
        if not isinstance(self.lat, (float, str)) or str(self.lat) == "":
            raise ValueError(
                "lat",
                f"'lat' must be a non-empty float or string, got {type(self.lat).__name__}",
            )

        if not isinstance(self.lon, (float, str)) or str(self.lon) == "":
            raise ValueError(
                "lon",
                f"'lon' must be a non-empty float or string, got {type(self.lon).__name__}",
            )

        self.radius = to_int(self.radius)
        self.zoom = to_int(self.zoom)

        if self.display_name is not None:
            self.display_name = to_str(self.display_name.strip()) or None

        self.lat = float(self.lat)
        self.lon = float(self.lon)
