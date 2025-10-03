import asyncio
import random
import string

from config import defaults
from contextlib import suppress
from .cli import cli_processor
from .exceptions import *
from .ssl import get_ssl_context
from components.logs import logger
from components.models.cluster import (
    ConnectionStatus,
    ErrorMessages,
    IncomingData,
    Role,
    MetaData,
    READER_DATA_PATTERN,
)
from components.utils import ntime_utc_now, unique_list, ensure_list

ALPHABET = string.ascii_lowercase + string.digits


class Server:
    def __init__(self, port):
        self.locks = dict()
        self.port = port
        self.callbacks = dict()
        self.temp_data = dict()
        self.stop_event = None
        self.server_limit = 104857600  # 100 MiB
        self.tasks = set()
        self.locking_timeout = 30.0  # time to spend acquiring a lock
        self._sending_incr = 0

    def register_command(self, plugin: "CommandPlugin"):
        self.registry.register(plugin)

    def _release_locks(self, lock_id: str, lock_objects: list | set):
        for l in ensure_list(unique_list(lock_objects)):
            if lock_id != self.locks[l]["id"]:
                logger.error("Cannot release due to id<>lock_object mismatch")
            elif l in self.locks:
                with suppress(RuntimeError):
                    self.locks[l]["lock"].release()
                self.locks[l]["id"] = None

    def _incoming_parser(self, input_bytes: bytes) -> IncomingData:
        try:
            input_decoded = input_bytes.strip().decode("utf-8")
            match = READER_DATA_PATTERN.search(input_decoded)
            data = match.groupdict()

            return IncomingData(
                ticket=data["ticket"],
                cmd=data["cmd"],
                payload=data["payload"],
                meta=MetaData(
                    cluster=data["cluster"],
                    leader=data["leader"],
                    started=data["started"],
                    name=data["name"],
                ),
            )
        except Exception as e:
            logger.critical(e)
            raise IncomingDataError(e)

    async def incoming_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        socket, *_ = writer.get_extra_info("socket").getsockname()
        raddr, *_ = writer.get_extra_info("peername")
        peer_init = False

        if socket and raddr in defaults.CLUSTER_CLI_BINDINGS:
            return await cli_processor((reader, writer))

        peer = self.peers.get_peer_by_raddr(raddr)

        if peer.streams.ingress:
            if peer.streams.ingress != (reader, writer):
                raise Exception(f"Duplicate connection from {raddr}/{peer.meta.name}")
        else:
            peer.streams.ingress = (reader, writer)

        while True:
            try:
                bytes_to_read = int.from_bytes(await reader.readexactly(4), "big")
                input_bytes = await reader.readexactly(bytes_to_read)
                logger.debug(f"Read {bytes_to_read + 4} bytes from {raddr}")

                data = self._incoming_parser(input_bytes)

                if data.meta.name != peer.name:
                    raise Exception(f"Expected {data.meta.name}, got {peer.name}")

                if not peer.streams.egress:
                    con, status = await self.peers.connect(data.meta.name)
                    if not con:
                        raise Exception(f"Error connecting {data.meta.name}: {status}")

                if peer.meta and (float(data.meta.started) < float(peer.meta.started)):
                    raise Exception(f"Inplausible started stamp from {data.meta.name}")

                peer.meta = data.meta

                if not peer_init and data.cmd != "BYE" and not self.stop_event.is_set():
                    peer_init = True
                    await self.monitor.peer(peer.meta.name)

                for plugin in self.registry.all():
                    if data.cmd == plugin.name:
                        reply_command = await plugin.dispatch(self, data)
                        if reply_command:
                            await self.send_command(
                                reply_command,
                                peer.meta.name,
                                ticket=data.ticket,
                            )
                        break
                else:
                    await self.send_command(
                        ErrorMessages.UNKNOWN_COMMAND.response,
                        peer.meta.name,
                        ticket=data.ticket,
                    )
            except CommandFailed:
                await self.send_command(
                    ErrorMessages.COMMAND_FAILED.response,
                    peer.meta.name,
                    ticket=data.ticket,
                )
                continue
            except (asyncio.exceptions.IncompleteReadError, ConnectionResetError) as e:
                logger.info(f"{raddr} closed connection: {e}")
                break
            except ClusterException as e:
                logger.error(f"Reading input data from {raddr} failed : {e}")
                break
            except Exception as e:
                logger.critical(f"Unhandled exception while reading from {raddr}: {e}")
                break
            except TimeoutError as e:
                if str(e) == "SSL shutdown timed out":
                    break
                raise

    async def send_command(
        self,
        cmd: str,
        peers: str | list = "*",
        ticket: str | None = None,
        raise_err=True,
        timeout: float = 5.0,
    ) -> str | None:
        if not self.stop_event:
            raise ServerNotRunning(self.stop_event)

        if not all(
            isinstance(v, t)
            for v, t in [
                (raise_err, bool),
                (timeout, (float, int)),
                (ticket, (str, type(None))),
                (peers, (str, list)),
                (cmd, str),
            ]
        ):
            raise ValueError("Invalid argument type")

        timeout = float(timeout)
        cmd_name, _, payload = cmd.partition(" ")

        valid_commands = {p.name for p in self.registry.all()}
        if cmd_name not in valid_commands:
            raise ValueError(f"Invalid command {cmd_name}")

        is_callback = cmd_name in {"OK", "ERR", "DATA"}
        requires_callback = not is_callback and cmd_name not in {"BYE", "INIT"}

        if self.stop_event.is_set() and cmd_name != "BYE":
            return None

        if is_callback:
            if not ticket:
                raise ValueError(f"Callback command {cmd_name} is missing ticket")
        elif not requires_callback:
            if ticket:
                raise ValueError(f"No-return command {cmd_name} cannot have a ticket")
            ticket = "NORET"
        else:
            if not ticket:
                ticket = f"{cmd_name}-{self.peers.local.name}-{self._sending_incr}"
                self._sending_incr += 1
            elif ticket in self.callbacks:
                raise ValueError(
                    f"Ticket {ticket} is already awaiting callbacks for {cmd_name}"
                )

            self.callbacks[ticket] = {
                "cmd": cmd_name,
                "responses": {},
                "failed_peers": set(),
                "receivers": set(),
            }

        final_peers = set()
        if peers == "*":
            for peer, remote in self.peers.remotes.items():
                if not remote.graceful_shutdown:
                    final_peers.add(peer)
        else:
            peers_to_check = peers if isinstance(peers, list) else [peers]
            for peer in peers_to_check:
                remote = self.peers.remotes.get(peer)
                if remote and not remote.graceful_shutdown:
                    final_peers.add(peer)
                elif not remote:
                    logger.warning(
                        f"Skipping unknown peer {peer} ({cmd_name}/{ticket})"
                    )
                elif remote.graceful_shutdown:
                    logger.warning(
                        f"Skipping shutdown peer {peer} ({cmd_name}/{ticket})"
                    )

        buffer_data = [
            ticket,
            f"{cmd_name} {payload}",
            ":META",
            f"NAME {self.peers.local.name}",
            f"CLUSTER {self.peers.local.cluster or '?CONFUSED'}",
            f"STARTED {self.peers.local.started}",
            f"LEADER {self.peers.local.leader or '?CONFUSED'}",
        ]
        buffer_bytes = " ".join(buffer_data).encode("utf-8")

        async def _write_data(peer_lock, writer, buffer_bytes):
            async with peer_lock:
                writer.write(len(buffer_bytes).to_bytes(4, "big"))
                writer.write(buffer_bytes)
                await writer.drain()

        writer_tasks = set()
        for peer in final_peers:
            con, status = await self.peers.connect(peer)
            if con:
                reader, writer = con
                writer_tasks.add(
                    _write_data(self.peers.remotes[peer].lock, writer, buffer_bytes)
                )
                if requires_callback:
                    self.callbacks[ticket]["responses"][peer] = None
                    self.callbacks[ticket]["receivers"].add(peer)
                    self.callbacks[ticket]["failed_peers"].add(peer)
            else:
                logger.error(f"Connection to peer {peer} failed: {status}")

        if requires_callback:
            if not self.callbacks[ticket]["receivers"]:
                logger.warning(f"Ticket {ticket} had no receivers")
                self.callbacks.pop(ticket, None)
                return True, {}
            self.callbacks[ticket]["barrier"] = asyncio.Barrier(
                len(self.callbacks[ticket]["receivers"]) + 1
            )

        await asyncio.gather(*writer_tasks)

        log = f"▲ {cmd_name} to {', '.join(final_peers)}"
        if is_callback:
            log += f", calling back {ticket}"
        elif requires_callback:
            log += f", requesting callback to {ticket}"
        logger.info(f"{log} ({len(buffer_bytes) + 4} bytes)")

        if not requires_callback:
            return True, {}

        try:
            async with asyncio.timeout(timeout):
                await self.callbacks[ticket]["barrier"].wait()
        except TimeoutError:
            logger.error(f"Timed out waiting for ticket {ticket} ({cmd_name})")
            await self.callbacks[ticket]["barrier"].abort()
        finally:
            callback_info = self.callbacks.pop(ticket, {})
            responses = callback_info.get("responses", {})
            failed_peers = callback_info.get("failed_peers", set())

            for peer in responses:
                responses[peer] = ErrorMessages._value2member_map_.get(
                    responses[peer], responses[peer]
                )

            if failed_peers:
                if raise_err:
                    raise ResponseError(responses)
                return False, responses

            return True, responses

    async def release(self, lock_id: str, lock_objects: list | set) -> None:
        if not isinstance(lock_id, str) or lock_id == "":
            raise ValueError(f"The 'lock_id' parameter must be a non-empty string")

        lock_objects = ensure_list(unique_list(lock_objects))

        if not lock_objects:
            raise ValueError(
                f"The 'lock_objects' parameter must be a non-empty list or set"
            )

        if self.peers.local.role == Role.FOLLOWER:
            try:
                await self.send_command(
                    f"UNLOCK {lock_id} {','.join(lock_objects)}",
                    self.peers.local.leader,
                    raise_err=True,
                )
            except ResponseError as e:
                raise LockException("Leader did not respond properly to unlock request")
        elif self.peers.local.role == Role.LEADER:
            self._release_locks(lock_id, lock_objects)

    async def acquire_lock(self, lock_objects: list | set) -> str:
        locked_objects = set()
        start = ntime_utc_now()
        lock_id = "".join(random.choices(ALPHABET, k=8))
        lock_objects = ensure_list(unique_list(lock_objects))

        if not lock_objects:
            raise ValueError(
                f"The 'lock_objects' parameter must be a non-empty list or set"
            )

        try:
            if self.peers.local.role == Role.LEADER:
                for l in lock_objects:
                    if l not in self.locks:
                        self.locks[l] = {
                            "lock": asyncio.Lock(),
                            "id": None,
                        }
                    await self.locks[l]["lock"].acquire(),
                    locked_objects.add(l)
                    self.locks[l]["id"] = lock_id
            elif self.peers.local.role == Role.FOLLOWER:
                if not self.peers.local.leader:
                    raise ClusterException("Leader is not elected yet")

                while (ntime_utc_now() - start) < self.locking_timeout:
                    result, responses = await self.send_command(
                        f"LOCK {lock_id} {','.join(lock_objects)}",
                        self.peers.local.leader,
                        raise_err=False,
                    )
                    if responses[self.peers.local.leader] == "BUSY":
                        await asyncio.sleep(0.1)
                        continue
                    elif not result:
                        if ErrorMessages.PEERS_MISMATCH in responses:
                            raise LockException("Lock rejected due to inconsistency")
                        else:
                            raise LockException(
                                f"Cannot acquire lock from leader ({responses})"
                            )
                    else:
                        break
                else:
                    raise LockException("Cannot acquire lock")

            return lock_id

        except Exception as e:
            if self.peers.local.role == Role.LEADER:
                self._release_locks(lock_id, locked_objects)
            if isinstance(e, ClusterException):
                raise
            elif isinstance(e, TimeoutError):
                raise LockException(f"Timeout acquiring local lock")
            raise LockException(f"Unhandled exception: {str(e)}")

    async def run(
        self, stop_event: asyncio.Event, post_stop_event: asyncio.Event
    ) -> None:
        self.server = await asyncio.start_server(
            self.incoming_handler,
            self.peers.local.server_bindings,
            self.port,
            ssl=get_ssl_context("server"),
            limit=self.server_limit,
        )

        self.stop_event = stop_event

        logger.info(
            f"Listening on {self.port} on address {' and '.join(self.peers.local.server_bindings)}..."
        )

        async with self.server:
            binds = [s.getsockname()[0] for s in self.server.sockets]
            for local_rbind in self.peers.local.server_bindings:
                if local_rbind not in binds:
                    logger.critical(f"Could not bind requested address {local_rbind}")
                    stop_event.set()
                    return

            sent = await self.send_command("INIT", "*")

            t = asyncio.create_task(self.monitor.server(), name="tickets")
            self.tasks.add(t)
            t.add_done_callback(self.tasks.discard)

            try:
                await stop_event.wait()
                raise asyncio.CancelledError("Shutting down")
            except asyncio.CancelledError:
                stop_event.set()
            finally:
                logger.info("Starting cluster shutdown")
                if self.peers.get_established():
                    try:
                        await self.send_command("BYE", "*")
                    except (asyncio.CancelledError, ConnectionResetError):
                        pass
                    except Exception as e:
                        logger.warning(f"Unhandled exception while sending BYE: {e}")

                for t in self.tasks.copy():
                    t.cancel()

                results = await asyncio.gather(*self.tasks, return_exceptions=True)

                if not all(
                    isinstance(e, asyncio.CancelledError) or e == None for e in results
                ):
                    logger.error(results)

                logger.info("Completed cluster shutdown, requesting app shutdown")
                post_stop_event.set()
