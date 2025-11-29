from PIL import Image
from io import BytesIO
from components.logs import logger


def convert_image_to_webp(
    image: str | bytes,
    save_as: str | None = None,
    max_width: int = 0,
    quality: int = 85,
    loseless: bool = True,
) -> bytes | None:
    logger.info("Compressing image to webp")
    if isinstance(image, bytes):
        img = Image.open(BytesIO(image))
    else:
        img = Image.open(image)
    width, height = img.size
    if max_width and width > max_width:
        new_height = int(max_width * height / width)
        img = img.resize((max_width, new_height), Image.LANCZOS)

    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    if save_as:
        img.save(save_as, format="WEBP", quality=quality, lossless=loseless)
        return None
    else:
        buffer = BytesIO()
        img.save(buffer, format="WEBP", quality=quality, lossless=loseless)
        return buffer.getvalue()
