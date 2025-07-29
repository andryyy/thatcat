import components.system
import json

from components.utils.requests import async_request
from components.models import Literal, validate_call


async def google_vision_api(
    base64_image: str, feature_types: list = ["TEXT_DETECTION"]
):
    settings = await components.system.get_system_settings()
    if not settings.details.GOOGLE_VISION_API_KEY:
        raise Exception("No Google Vision API key found in settings")

    status_code, response_text = await async_request(
        f"https://vision.googleapis.com/v1/images:annotate?key={settings.details.GOOGLE_VISION_API_KEY}",
        method="POST",
        data={
            "requests": [
                {
                    "image": {"content": base64_image},
                    "features": [{"type": ft} for ft in feature_types],
                }
            ]
        },
        headers={"Content-Type": "application/json"},
    )

    if status_code >= 400:
        try:
            response_text = json.loads(response_text)
            error = response_text.get("error")
            error = json.dumps(error)
        except:
            error = response_text
        finally:
            raise Exception(error)
    else:
        return json.loads(response_text)
