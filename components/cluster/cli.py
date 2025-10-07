import asyncio
import json
import random
from components.database.states import STATE
from components.database import db


async def cli_processor(streams: tuple[asyncio.StreamReader, asyncio.StreamWriter]):
    try:
        reader, writer = streams
        while not reader.at_eof():
            cmd = await reader.readexactly(1)
            if cmd == b"\x97":
                data = await reader.readuntil(b"\n")
                login = data.strip().decode("utf-8")
                try:
                    async with db:
                        user = await db.search(
                            "users",
                            {"login": login},
                        )
                    if user:
                        user = user[0]
                        if "system" not in user["acl"]:
                            STATE.promote_users.add(user["id"])
                            writer.write(b"\x01")
                        else:
                            writer.write(b"\x02")
                    else:
                        writer.write(b"\x03")
                except Exception as e:
                    writer.write(b"\x03")
                await writer.drain()
            elif cmd == b"\x98":
                awaiting = dict()
                for idx, (k, v) in enumerate(STATE.terminal_tokens.items(), start=1):
                    awaiting[idx] = (k, v["intention"])

                writer.write(f"{json.dumps(awaiting)}\n".encode("ascii"))
                await writer.drain()
            elif cmd == b"\x99":
                data = await reader.readexactly(14)
                confirmed = data.strip().decode("ascii")
                code = "%06d" % random.randint(0, 999999)
                if confirmed in STATE.terminal_tokens:
                    STATE.terminal_tokens.get(confirmed, {}).update(
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
        print(111)
        writer.close()
        await writer.wait_closed()
