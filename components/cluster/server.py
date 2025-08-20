import asyncio
import re

from config import defaults
from contextlib import suppress
from .cli import cli_processor
from .exceptions import *
from .ssl import get_ssl_context
from components.logs import logger
from components.models.cluster import (
    confloat,
    Callable,
    ConnectionStatus,
    CritErrors,
    IncomingData,
    Role,
    SendCommandReturn,
    validate_call,
    ValidationError,
)
from components.utils import ensure_unique_list, ntime_utc_now


class Server:
    def __init__(self, port):
        self.locks = dict()
        self.port = port
        self.callback_tickets = dict()
        self.temp_data = dict()
        self.stop_event = None
        self.server_limit = 104857600  # 100 MiB
        self.tasks = set()
        self.locking_timeout = 10.0  # time to spend acquiring a lock
        self.lock_timeout = 4  # ttl for an acquired lock
        self.receiving = asyncio.Condition()

    def register_command(self, plugin: "CommandPlugin"):
        self.registry.register(plugin)

    def _release_tables(self, lock_id, tables):
        for t in tables:
            if lock_id != self.locks[t]["id"]:
                logger.error("Table release failed due to lock id mismatch")
            elif t in self.locks:
                with suppress(RuntimeError):
                    self.locks[t]["lock"].release()
                self.locks[t]["id"] = None

    async def read_command(
        self, reader: asyncio.StreamReader, raddr: str
    ) -> tuple[str, str, dict]:
        bytes_to_read = int.from_bytes(await reader.readexactly(4), "big")
        input_bytes = await reader.readexactly(bytes_to_read)

        input_decoded = input_bytes.strip().decode("utf-8")
        data, _, meta = input_decoded.partition(" :META ")
        ticket, _, cmd = data.partition(" ")

        patterns = [
            r"NAME (?P<name>\S+)",
            r"CLUSTER (?P<cluster>\S+)",
            r"STARTED (?P<started>\S+)",
            r"LEADER (?P<leader>\S+)",
        ]

        match = re.search(" ".join(patterns), meta)
        meta_dict = match.groupdict()
        name = meta_dict["name"]

        if not name in self.peers.remotes:
            raise UnknownPeer(name)

        if raddr not in self.peers.remotes[name].ips:
            raise UnknownPeer(raddr)

        self.peers.remotes[name].leader = meta_dict["leader"]
        self.peers.remotes[name].started = float(meta_dict["started"])
        self.peers.remotes[name].cluster = meta_dict["cluster"]

        msg = cmd[:150] + (cmd[150:] and "...")
        logger.debug(f"← [{name}][{ticket}] - {msg}")

        return IncomingData(ticket=ticket, cmd=cmd, sender=name)

    async def incoming_handler(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        peer_init = False
        raddr, *_ = writer.get_extra_info("peername")
        socket, *_ = writer.get_extra_info("socket").getsockname()

        if socket and raddr in defaults.CLUSTER_CLI_BINDINGS:
            return await cli_processor((reader, writer))

        while True:
            try:
                incoming_data = await self.read_command(reader, raddr)

                if not peer_init and not incoming_data.cmd == "BYE":
                    if self.peers.remotes[incoming_data.sender].streams.ingress:
                        await self.send_command(
                            CritErrors.ZOMBIE.response,
                            incoming_data.sender,
                            ticket=incoming_data.ticket,
                        )
                        break
                    self.peers.remotes[incoming_data.sender].streams.ingress = (
                        reader,
                        writer,
                    )
                    self.peers.remotes[incoming_data.sender].graceful_shutdown = False
                    await self.monitor.peer(incoming_data.sender)
                    peer_init = True

            except (asyncio.exceptions.IncompleteReadError, ConnectionResetError):
                break
            except TimeoutError as e:
                if str(e) == "SSL shutdown timed out":
                    break
                raise
            except ValidationError as e:
                logger.critical(e)
                logger.error(f"Invalid data received from {raddr}")
                continue
            except MonitoringTaskExists:
                await self.send_command(
                    CritErrors.ZOMBIE.response,
                    incoming_data.sender,
                    ticket=incoming_data.ticket,
                )
                break

            try:
                for plugin in self.registry.all():
                    if incoming_data.cmd.startswith(plugin.name):
                        reply_command = await plugin.dispatch(self, incoming_data)
                        if reply_command:
                            await self.send_command(
                                reply_command,
                                incoming_data.sender,
                                ticket=incoming_data.ticket,
                            )
                        break
                else:
                    await self.send_command(
                        CritErrors.UNKNOWN_COMMAND.response,
                        incoming_data.sender,
                        ticket=incoming_data.ticket,
                    )

                async with self.receiving:
                    self.receiving.notify_all()

            except ClusterCommandFailed:
                await self.send_command(
                    CritErrors.COMMAND_FAILED.response,
                    incoming_data.sender,
                    ticket=incoming_data.ticket,
                )
            except ConnectionResetError:
                break

    @validate_call
    async def send_command(
        self,
        cmd,
        peers: str | list | None = None,
        ticket: str | None = None,
    ) -> SendCommandReturn | None:
        if not self.stop_event:
            raise ServerNotRunning()

        if self.stop_event.is_set() and cmd != "BYE":
            logger.warning(
                f"[→ NOT sending BYE commands while shutting down [{ticket}]"
            )
            return SendCommandReturn(ticket="", receivers=[])

        if not ticket:
            ticket = ntime_utc_now()
        ticket = str(ticket)

        if ticket not in self.callback_tickets and not cmd.startswith("ACK"):
            self.callback_tickets[ticket] = set()

        if not peers or peers == "*":
            peers = [
                p
                for p in self.peers.remotes.keys()
                if not self.peers.remotes[p].graceful_shutdown
            ]

        receivers = []
        for name in ensure_unique_list(peers):
            if name not in self.peers.remotes:
                raise UnknownPeer(name)

            async with self.peers.remotes[name].lock:
                if self.peers.remotes[name].graceful_shutdown:
                    logger.warning(
                        f"[→ NOT sending to {name}][{ticket}] - Peer left gracefully"
                    )
                    continue

                con, status = await self.peers.connect(name)
                if con:
                    reader, writer = con
                    buffer_data = [
                        ticket,
                        cmd,
                        ":META",
                        f"NAME {self.peers.local.name}",
                        "CLUSTER {cluster}".format(
                            cluster=self.peers.local.cluster or "?CONFUSED"
                        ),
                        f"STARTED {self.peers.local.started}",
                        "LEADER {leader}".format(
                            leader=self.peers.local.leader or "?CONFUSED"
                        ),
                    ]
                    buffer_bytes = " ".join(buffer_data).encode("utf-8")
                    writer.write(len(buffer_bytes).to_bytes(4, "big"))
                    writer.write(buffer_bytes)
                    await writer.drain()

                    msg = cmd[:150] + (cmd[150:] and "...")
                    logger.debug(f"→ [{name}][{ticket}] - {msg}")

                    receivers.append(name)
                else:
                    logger.warning(f"Cannot send to peer {name} - {status}")

        return SendCommandReturn(ticket=ticket, receivers=receivers)

    @validate_call
    async def await_receivers(
        self,
        send_command_return: SendCommandReturn,
        raise_err: bool = True,
        timeout: confloat(le=10.0) = 5.0,
    ):
        ticket = send_command_return.ticket
        receivers = send_command_return.receivers

        if not receivers:
            logger.warning(f"Ticket {ticket} had no receivers")
            self.callback_tickets.pop(ticket, None)
            return True, {}

        if not ticket in self.callback_tickets:
            raise IncompleteClusterResponses("Ticket is not awaiting callbacks")

        try:
            async with asyncio.timeout(timeout):
                callbacks = self.callback_tickets.get(ticket, set())
                while not all(r in [p for p, _ in callbacks] for r in receivers):
                    await self.receiving.wait()
                    callbacks = self.callback_tickets.get(ticket, set())
        except TimeoutError:
            logger.error("Timeout waiting for receivers")
        finally:
            self.callback_tickets.pop(ticket, None)
            responses = {
                peer: CritErrors(response)
                if response in CritErrors._value2member_map_
                else response
                for peer, response in callbacks
            }
            responses["_missing"] = [
                r for r in receivers if r not in [p for p, _ in callbacks]
            ]

            if (
                any(isinstance(r, CritErrors) for r in responses.values())
                or responses["_missing"]
            ):
                logger.error(responses)
                if raise_err:
                    raise IncompleteClusterResponses(responses)
                return False, responses

            return True, responses

    async def release(self, lock_id, tables: list = ["main"]) -> str:
        if self.peers.local.role == Role.FOLLOWER:
            try:
                async with self.receiving:
                    sent = await self.send_command(
                        f"UNLOCK {lock_id} {','.join(tables)}", self.peers.local.leader
                    )
                    await self.await_receivers(sent, raise_err=True)
            except IncompleteClusterResponses:
                raise LockException("Leader did not respond properly to unlock request")
        elif self.peers.local.role == Role.LEADER:
            self._release_tables(lock_id, tables)

    async def acquire_lock(self, tables: list) -> str:
        locked_tables = set()
        lock_id = str(ntime_utc_now())

        try:
            if self.peers.local.role == Role.LEADER:
                for t in tables:
                    if t not in self.locks:
                        self.locks[t] = {
                            "lock": asyncio.Lock(),
                            "id": None,
                        }
                    await self.locks[t]["lock"].acquire(),
                    locked_tables.add(t)
                    self.locks[t]["id"] = lock_id
            elif self.peers.local.role == Role.FOLLOWER:
                if not self.peers.local.leader:
                    raise ClusterException("Leader is not elected yet")

                while (ntime_utc_now() - float(lock_id)) < self.locking_timeout:
                    async with self.receiving:
                        sent = await self.send_command(
                            f"LOCK {lock_id} {','.join(tables)}",
                            self.peers.local.leader,
                        )
                        result, responses = await self.await_receivers(
                            sent, raise_err=False
                        )

                    responses = list(responses.values())
                    if "BUSY" in responses:
                        continue
                    elif not result:
                        if CritErrors.PEERS_MISMATCH in responses:
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
                self._release_tables(lock_id, locked_tables)
            if isinstance(e, ClusterException):
                raise
            elif isinstance(e, TimeoutError):
                raise LockException(f"Timeout acquiring local lock")
            raise LockException(f"Unhandled exception: {str(e)}")

    async def run(self, stop_event: Callable, post_stop_event: Callable) -> None:
        server = await asyncio.start_server(
            self.incoming_handler,
            self.peers.local._all_bindings_as_str,
            self.port,
            ssl=get_ssl_context("server"),
            limit=self.server_limit,
        )

        self.stop_event = stop_event

        logger.info(
            f"Listening on {self.port} on address {' and '.join(self.peers.local._all_bindings_as_str)}..."
        )

        async with server:
            binds = [s.getsockname()[0] for s in server.sockets]
            for local_rbind in self.peers.local._bindings_as_str:
                if local_rbind not in binds:
                    logger.critical(f"Could not bind requested address {local_rbind}")
                    stop_event.set()
                    return

            async with self.receiving:
                sent = await self.send_command("INIT", "*")
                _, responses = await self.await_receivers(sent, raise_err=False)

            if CritErrors.ZOMBIE in responses.values():
                logger.critical(
                    f"Peer {name} has not yet disconnected a previous session: {status}"
                )
                stop_event.set()

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
                    except ConnectionResetError:
                        pass

                for t in self.tasks.copy():
                    t.cancel()

                results = await asyncio.gather(*self.tasks, return_exceptions=True)

                if not all(
                    isinstance(e, asyncio.CancelledError) or e == None for e in results
                ):
                    logger.error(results)

                logger.info("Completed cluster shutdown, requesting app shutdown")
                post_stop_event.set()
