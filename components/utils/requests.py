import asyncio
import json
import urllib.error
import urllib.request


async def async_request(
    url: str,
    method: str,
    data: dict = {},
    headers: dict = {},
):
    if not isinstance(url, str):
        raise ValueError(f"'url' must be a string, but got {type(url).__name__}.")

    if not isinstance(method, str) or method not in ["GET", "POST", "PATCH", "DELETE"]:
        raise ValueError(f"'method' must be a string, but got {type(method).__name__}.")

    if not isinstance(data, dict):
        raise ValueError(
            f"'data' parameter must be a dictionary, but got {type(data).__name__}."
        )

    if not isinstance(headers, dict):
        raise ValueError(
            f"'headers' parameter must be a dictionary, but got {type(headers).__name__}."
        )

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


def sync_request(
    url: str,
    method: str,
    data: dict = {},
    headers: dict = {},
):
    if not isinstance(url, str):
        raise ValueError(f"'url' must be a string, but got {type(url).__name__}.")

    if not isinstance(method, str) or method not in ["GET", "POST", "PATCH", "DELETE"]:
        raise ValueError(f"'method' must be a string, but got {type(method).__name__}.")

    if not isinstance(data, dict):
        raise ValueError(
            f"'data' parameter must be a dictionary, but got {type(data).__name__}."
        )

    if not isinstance(headers, dict):
        raise ValueError(
            f"'headers' parameter must be a dictionary, but got {type(headers).__name__}."
        )

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
