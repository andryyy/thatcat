import asyncio
import json
import urllib.error
import urllib.request

from components.models import Literal, validate_call


@validate_call
async def async_request(
    url: str,
    method: str = Literal["GET", "POST", "PATCH", "DELETE"],
    data: dict = {},
    headers: dict = {},
):
    def _blocking_request():
        try:
            req = urllib.request.Request(
                url,
                data=(json.dumps(data).encode("utf-8") if data else None),
                headers=headers,
                method=method,
            )
            with urllib.request.urlopen(req) as response:
                return response.getcode(), response.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode("utf-8")

    return await asyncio.to_thread(_blocking_request)
