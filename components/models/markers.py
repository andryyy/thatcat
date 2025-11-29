from dataclasses import dataclass
from components.models.helpers import to_int, to_float, to_str, hex_color_validator


@dataclass
class CarMarker:
    id: int
    color: str
    x: float
    y: float
    name: str | None = None

    def __post_init__(self):
        self.id = to_int(self.id)
        if not hex_color_validator(self.color):
            raise ValueError(
                "color", "'color' must be a #-prefixed hexadecimal color code"
            )

        if self.name is not None:
            self.name = to_str(self.name.strip()) or None

        self.x = to_float(self.x)
        self.y = to_float(self.y)
