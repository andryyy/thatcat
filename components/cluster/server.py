"""Cluster server module for managing peer-to-peer communication and distributed locking."""

import asyncio
import random

from .base import ALPHABET, MESSAGE_SIZE_BYTES, ServerBase
from .cli import cli_processor
from .exceptions import ClusterException, CommandFailed, LockException, ResponseError
from .models import ErrorMessages, Role
from .ssl import get_ssl_context
from components.logs import logger
from components.utils.datetimes import ntime_utc_now
from components.utils.misc import ensure_list, unique_list

# Server configuration constants
DEFAULT_SERVER_LIMIT = 104857600  # 100 MiB
DEFAULT_LOCKING_TIMEOUT = 30.0  # seconds
DEFAULT_COMMAND_TIMEOUT = 5.0  # seconds
LOCK_ID_LENGTH = 8


class Server(ServerBase):
    def __init__(self, port):
        self.locks = dict()
        self.port = port
        self.callbacks = dict()
        self.temp_data = dict()
        self.shutdown_trigger = None
        self.server_limit = DEFAULT_SERVER_LIMIT
        self.tasks = set()
        self.locking_timeout = DEFAULT_LOCKING_TIMEOUT
        self._sending_incr = 0
        self._init_completed = asyncio.Event()

    async def incoming_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        await self._init_completed.wait()
        if self.shutdown_trigger.is_set():
            raise Exception("Server is not accepting new connections")

        socket, *_ = writer.get_extra_info("socket").getsockname()
        raddr, *_ = writer.get_extra_info("peername")

        if socket and raddr in self.peers.local.cli_bindings:
            return await cli_processor((reader, writer))

        peer = self.peers.get_peer_by_raddr(raddr)

        if peer.streams.ingress:
            if peer.streams.ingress != (reader, writer):
                raise Exception(f"Duplicate connection from {raddr}/{peer.name}")
        peer.streams.ingress = (reader, writer)

        if not peer.streams.egress:
            con, status = await self.peers.connect(peer.name)
            if not con:
                connection_status, exc = status
                raise Exception(
                    f"Error connecting egress after ingress from {peer.name}: {connection_status}"
                ) from exc

        await self.watchdog.peer(peer)

        while True:
            try:
                bytes_to_read = int.from_bytes(
                    await reader.readexactly(MESSAGE_SIZE_BYTES), "big"
                )
                input_bytes = await reader.readexactly(bytes_to_read)
                logger.debug(
                    f"Read {bytes_to_read + MESSAGE_SIZE_BYTES} bytes from {raddr}"
                )

                data = self._incoming_parser(input_bytes)
                self._peer_meta_update(peer, data)

                await self._process_command(data, peer.meta.name)
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
            except TimeoutError as e:
                if str(e) == "SSL shutdown timed out":
                    break
                raise
            except Exception as e:
                logger.critical(f"Error while reading from {raddr}: {e}")
                raise

    async def send_command(
        self,
        cmd: str,
        peers: str | list = "*",
        ticket: str | None = None,
        raise_err=True,
        timeout: float = DEFAULT_COMMAND_TIMEOUT,
    ) -> str | None:
        """Send a command to one or more peers and optionally wait for response."""
        self._validate_send_command_params(cmd, peers, ticket, raise_err, timeout)
        timeout = float(timeout)
        cmd_name, _, payload = cmd.partition(" ")

        if cmd_name not in self.registry.commands:
            raise ValueError(f"Invalid command {cmd_name}")

        is_callback = cmd_name in self.registry.callback_commands
        requires_callback = cmd_name in self.registry.requires_callback

        if self.shutdown_trigger.is_set() and cmd_name != "BYE":
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

        buffer_bytes = self._build_message_buffer(ticket, cmd_name, payload)

        async def _write_data(peer_lock, writer, buffer_bytes):
            async with peer_lock:
                writer.write(len(buffer_bytes).to_bytes(MESSAGE_SIZE_BYTES, "big"))
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
        logger.info(f"{log} ({len(buffer_bytes) + MESSAGE_SIZE_BYTES} bytes)")

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
        """Release distributed locks across the cluster."""
        if not isinstance(lock_id, str) or lock_id == "":
            raise ValueError("The 'lock_id' parameter must be a non-empty string")

        lock_objects = ensure_list(unique_list(lock_objects))

        if not lock_objects:
            raise ValueError(
                "The 'lock_objects' parameter must be a non-empty list or set"
            )

        if self.peers.local.role == Role.FOLLOWER:
            try:
                await self.send_command(
                    f"UNLOCK {lock_id} {','.join(lock_objects)}",
                    self.peers.local.leader,
                    raise_err=True,
                )
            except ResponseError:
                raise LockException("Leader did not respond properly to unlock request")
        elif self.peers.local.role == Role.LEADER:
            self._release_locks(lock_id, lock_objects)

    async def acquire_lock(self, lock_objects: list | set) -> str:
        """Acquire distributed locks across the cluster. Returns lock_id."""
        lock_objects = ensure_list(unique_list(lock_objects))

        if not lock_objects:
            raise ValueError(
                "The 'lock_objects' parameter must be a non-empty list or set"
            )

        lock_id = "".join(random.choices(ALPHABET, k=LOCK_ID_LENGTH))
        start = ntime_utc_now()

        try:
            if self.peers.local.role == Role.LEADER:
                await self._acquire_leader_locks(lock_id, lock_objects)
            elif self.peers.local.role == Role.FOLLOWER:
                await self._acquire_follower_locks(lock_id, lock_objects, start)

            return lock_id

        except Exception as e:
            if isinstance(e, (ClusterException, LockException)):
                raise
            elif isinstance(e, TimeoutError):
                raise LockException("Timeout acquiring local lock")
            raise LockException(f"Unhandled exception: {str(e)}")

    async def run(self, shutdown_trigger: asyncio.Event) -> None:
        self.server = await asyncio.start_server(
            self.incoming_handler,
            self.peers.local.server_bindings,
            self.port,
            ssl=get_ssl_context("server"),
            limit=self.server_limit,
        )

        self.shutdown_trigger = shutdown_trigger
        async with self.server:
            binds = [s.getsockname()[0] for s in self.server.sockets]
            for local_rbind in self.peers.local.server_bindings:
                if local_rbind not in binds:
                    logger.critical(f"Could not bind requested address {local_rbind}")
                    shutdown_trigger.set()
                    return

            logger.info(f"Listening on {binds}")

            await self.send_command("INIT", "*")
            self._init_completed.set()

            asyncio.create_task(self.watchdog.server(), name="tickets")

            try:
                await shutdown_trigger.wait()
                self.server.close()
            except asyncio.CancelledError:
                shutdown_trigger.set()
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
                    isinstance(e, asyncio.CancelledError) or e is None for e in results
                ):
                    logger.error(results)

                self.server.abort_clients()
                await self.server.wait_closed()
