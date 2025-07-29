import magic
import math
from io import BytesIO
from PIL import Image, UnidentifiedImageError
from PIL.ExifTags import GPSTAGS, TAGS


class ImageExif:
    def __init__(self, image_bytes: bytes):
        self.image_bytes = image_bytes
        self.image = Image.open(image_bytes)
        self.gps_info = {}
        self.exif_data = self.image._getexif()
        if not self.exif_data:
            raise ValueError("No EXIF data")

    @staticmethod
    def _convert_to_degrees(value):
        d, m, s = value
        degrees = float(d)
        minutes = float(m)
        seconds = float(s)
        return degrees + (minutes / 60.0) + (seconds / 3600.0)

    @staticmethod
    def _is_invalid_gps(value):
        for v in value:
            try:
                result = float(v)
                if math.isnan(result):
                    return True
            except ZeroDivisionError:
                return True
            except Exception:
                return True
        return False

    def _load_gps_info(self):
        for exif_tag, exif_value in self.exif_data.items():
            if TAGS.get(exif_tag, exif_tag) == "GPSInfo":
                for gps_key, gps_value in exif_value.items():
                    gps_tag = GPSTAGS.get(gps_key, gps_key)
                    if gps_tag in [
                        "GPSLatitude",
                        "GPSLongitude",
                        "GPSLatitudeRef",
                        "GPSLongitudeRef",
                    ]:
                        if gps_tag in [
                            "GPSLatitude",
                            "GPSLongitude",
                        ] and self._is_invalid_gps(gps_value):
                            return {}
                        self.gps_info[gps_tag] = gps_value

    @property
    def coords(self):
        self._load_gps_info()
        if not self.gps_info:
            raise ValueError("No GPS info")
        try:
            lat = self._convert_to_degrees(self.gps_info["GPSLatitude"])
            lat_ref = self.gps_info["GPSLatitudeRef"]
            if lat_ref != "N":
                lat = -lat

            lon = self._convert_to_degrees(self.gps_info["GPSLongitude"])
            lon_ref = self.gps_info["GPSLongitudeRef"]
            if lon_ref != "E":
                lon = -lon
            return f"{lat},{lon}"
        except KeyError:
            return None


def image_bytes_to_webp(
    image_bytes: bytes, max_width: int = 0, quality: int = 85
) -> bytes:
    with Image.open(BytesIO(image_bytes)) as img:
        width, height = img.size

        if max_width and width > max_width:
            new_height = int(max_width * height / width)
            img = img.resize((max_width, new_height), Image.LANCZOS)

        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        output_io = BytesIO()
        img.save(output_io, format="WEBP", quality=quality)
        return output_io.getvalue()


def convert_file_to_webp(file_path: str, max_width: int = 0, quality: int = 85) -> None:
    with Image.open(file_path) as img:
        width, height = img.size

        if max_width and width > max_width:
            new_height = int(max_width * height / width)
            img = img.resize((max_width, new_height), Image.LANCZOS)

        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        img.save(file_path, format="WEBP", quality=quality)
