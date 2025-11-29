podman run --network pasta -p 443:443 --rm -it -v $(pwd):/data ghcr.io/astral-sh/uv:debian uvx --with quart,msgpack,python-magic,cbor2,ecdsa python3.13 /data/main.py
