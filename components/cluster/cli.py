import asyncio
import json
import random


async def cli_processor(streams: tuple[asyncio.StreamReader, asyncio.StreamWriter]):
    from components.users import what_id, get
    from components.database import IN_MEMORY_DB

    try:
        reader, writer = streams
        while not reader.at_eof():
            cmd = await reader.readexactly(1)
            if cmd == b"\x97":
                data = await reader.readuntil(b"\n")
                login = data.strip().decode("utf-8")
                try:
                    user_id = await what_id(login=login)
                    user = await get(user_id=user_id)
                    if "system" not in user.acl:
                        IN_MEMORY_DB["PROMOTE_USERS"].add(user_id)
                        writer.write(b"\x01")
                    else:
                        writer.write(b"\x02")
                except Exception as e:
                    writer.write(b"\x03")
                await writer.drain()
            elif cmd == b"\x98":
                awaiting = dict()
                tokens = (
                    IN_MEMORY_DB["TOKENS"]["LOGIN"] | IN_MEMORY_DB["TOKENS"]["REGISTER"]
                )
                for idx, (k, v) in enumerate(tokens.items(), start=1):
                    awaiting[idx] = (k, v["intention"])

                writer.write(f"{json.dumps(awaiting)}\n".encode("ascii"))
                await writer.drain()
            elif cmd == b"\x99":
                data = await reader.readexactly(14)
                confirmed = data.strip().decode("ascii")
                code = "%06d" % random.randint(0, 999999)
                if confirmed in IN_MEMORY_DB["TOKENS"]["LOGIN"]:
                    IN_MEMORY_DB["TOKENS"]["LOGIN"].get(confirmed, {}).update(
                        {"status": "confirmed", "code": code}
                    )
                elif confirmed in IN_MEMORY_DB["TOKENS"]["REGISTER"]:
                    IN_MEMORY_DB["TOKENS"]["REGISTER"].get(confirmed, {}).update(
                        {"status": "confirmed", "code": code}
                    )
                writer.write(f"{code}\n".encode("ascii"))
                await writer.drain()
    except Exception as e:
        if type(e) not in [
            asyncio.exceptions.IncompleteReadError,
            ConnectionResetError,
        ]:
            raise
    finally:
        writer.close()
        await writer.wait_closed()
