"""Base class for cluster server with helper methods."""

import asyncio
import string
from contextlib import suppress

from components.logs import logger
from components.models.cluster import (
    ErrorMessages,
    IncomingData,
    MetaData,
    READER_DATA_PATTERN,
)
from components.utils.datetimes import ntime_utc_now
from components.utils.misc import unique_list, ensure_list
from .exceptions import ClusterException, LockException, IncomingDataError

# Constants
ALPHABET = string.ascii_lowercase + string.digits
MESSAGE_SIZE_BYTES = 4
LOCK_RETRY_DELAY = 0.1


class ServerBase:
    """Base class providing helper methods for cluster server operations."""

    def _release_locks(self, lock_id: str, lock_objects: list | set):
        """Release locks held by the given lock_id."""
        for lock_obj in ensure_list(unique_list(lock_objects)):
            if lock_obj not in self.locks:
                continue

            if lock_id != self.locks[lock_obj]["id"]:
                logger.error(f"Cannot release lock {lock_obj}: id mismatch")
                continue

            with suppress(RuntimeError):
                self.locks[lock_obj]["lock"].release()
            self.locks[lock_obj]["id"] = None

    async def _acquire_leader_locks(
        self, lock_id: str, lock_objects: list, timeout: float | None = None
    ) -> set:
        """Acquire locks as leader. Returns set of acquired lock objects."""
        locked_objects = set()

        for lock_obj in lock_objects:
            if lock_obj not in self.locks:
                self.locks[lock_obj] = {
                    "lock": asyncio.Lock(),
                    "id": None,
                }

        try:
            for lock_obj in lock_objects:
                if timeout is not None:
                    await asyncio.wait_for(
                        self.locks[lock_obj]["lock"].acquire(), timeout
                    )
                else:
                    await self.locks[lock_obj]["lock"].acquire()
                locked_objects.add(lock_obj)
                self.locks[lock_obj]["id"] = lock_id

            return locked_objects
        except Exception:
            self._release_locks(lock_id, locked_objects)
            raise

    async def _acquire_follower_locks(
        self, lock_id: str, lock_objects: list, start: float
    ):
        """Acquire locks as follower by requesting from leader."""
        if not self.peers.local.leader:
            raise ClusterException("Leader is not elected yet")

        while (ntime_utc_now() - start) < self.locking_timeout:
            result, responses = await self.send_command(
                f"LOCK {lock_id} {','.join(lock_objects)}",
                self.peers.local.leader,
                raise_err=False,
            )
            if responses[self.peers.local.leader] == "BUSY":
                await asyncio.sleep(LOCK_RETRY_DELAY)
                continue
            elif not result:
                if ErrorMessages.PEERS_MISMATCH in responses.values():
                    raise LockException("Lock rejected due to inconsistency")
                else:
                    raise LockException(
                        f"Cannot acquire lock from leader ({responses})"
                    )
            else:
                return
        else:
            raise LockException("Cannot acquire lock: timeout")

    def _incoming_parser(self, input_bytes: bytes) -> IncomingData:
        """Parse incoming message bytes into structured data."""
        try:
            input_decoded = input_bytes.strip().decode("utf-8")
            match = READER_DATA_PATTERN.search(input_decoded)
            if not match:
                raise ValueError("Message does not match expected pattern")

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
            logger.critical(f"Failed to parse incoming data: {e}")
            raise IncomingDataError(e)

    async def _validate_and_setup_peer(self, peer, raddr, data: IncomingData) -> bool:
        """Validate peer metadata and establish egress connection if needed."""
        if data.meta.name != peer.name:
            raise Exception(f"Expected {data.meta.name}, got {peer.name}")

        if not peer.streams.egress:
            con, status = await self.peers.connect(data.meta.name)
            if not con:
                raise Exception(f"Error connecting {data.meta.name}: {status}")

        if peer.meta and (float(data.meta.started) < float(peer.meta.started)):
            raise Exception(f"Inplausible started stamp from {data.meta.name}")

        peer.meta = data.meta
        return True

    async def _process_command(self, data: IncomingData, peer_name: str):
        """Dispatch command to appropriate plugin and send reply if needed."""
        for plugin in self.registry.all():
            if data.cmd == plugin.name:
                reply_command = await plugin.dispatch(self, data)
                if reply_command:
                    await self.send_command(
                        reply_command,
                        peer_name,
                        ticket=data.ticket,
                    )
                break
        else:
            await self.send_command(
                ErrorMessages.UNKNOWN_COMMAND.response,
                peer_name,
                ticket=data.ticket,
            )

    def _validate_send_command_params(
        self,
        cmd: str,
        peers: str | list,
        ticket: str | None,
        raise_err: bool,
        timeout: float,
    ):
        """Validate send_command parameters."""
        from .exceptions import ServerNotRunning

        if not self.shutdown_trigger:
            raise ServerNotRunning(self.shutdown_trigger)

        if not isinstance(cmd, str):
            raise ValueError("cmd must be a string")
        if not isinstance(peers, (str, list)):
            raise ValueError("peers must be a string or list")
        if ticket is not None and not isinstance(ticket, str):
            raise ValueError("ticket must be a string or None")
        if not isinstance(raise_err, bool):
            raise ValueError("raise_err must be a boolean")
        if not isinstance(timeout, (float, int)):
            raise ValueError("timeout must be a float or int")

    def _build_message_buffer(self, ticket: str, cmd_name: str, payload: str) -> bytes:
        """Build the message buffer to send to peers."""
        buffer_data = [
            ticket,
            f"{cmd_name} {payload}",
            ":META",
            f"NAME {self.peers.local.name}",
            f"CLUSTER {self.peers.local.cluster or '?CONFUSED'}",
            f"STARTED {self.peers.local.started}",
            f"LEADER {self.peers.local.leader or '?CONFUSED'}",
        ]
        return " ".join(buffer_data).encode("utf-8")
