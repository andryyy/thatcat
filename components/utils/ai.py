import json

from .requests import async_request


async def google_vision_api(
    api_key: str, base64_image: str, feature_types: list = ["TEXT_DETECTION"]
):
    status_code, response_text = await async_request(
        f"https://vision.googleapis.com/v1/images:annotate?key={api_key}",
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
