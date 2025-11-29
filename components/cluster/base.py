"""Base class for cluster server with helper methods."""

import asyncio
import string
from contextlib import suppress

from components.logs import logger
from .models import ErrorMessages, IncomingData, MetaData, READER_DATA_PATTERN
from components.utils.datetimes import ntime_utc_now
from components.utils.misc import unique_list, ensure_list
from .exceptions import (
    ClusterException,
    LockException,
    IncomingDataError,
    MetaDataError,
    ServerNotRunning,
)

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
                if ErrorMessages.NOT_READY in responses.values():
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
                    state=data["state"],
                    name=data["name"],
                ),
            )
        except Exception as e:
            logger.critical(f"Failed to parse incoming data: {e}")
            raise IncomingDataError(e)

    def _peer_meta_update(self, peer, data: IncomingData) -> bool:
        if data.meta.name != peer.name:
            raise MetaDataError(f"Expected {data.meta.name}, got {peer.name}")

        if peer.meta and (float(data.meta.started) < float(peer.meta.started)):
            raise MetaDataError(f"Inplausible started stamp from {data.meta.name}")

        peer.meta = data.meta

    async def _process_command(self, data: IncomingData, peer_name: str):
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
        buffer_data = [
            ticket,
            f"{cmd_name} {payload}",
            ":META",
            f"NAME {self.peers.local.name}",
            f"CLUSTER {self.peers.local.cluster or '?CONFUSED'}",
            f"STARTED {self.peers.local.started}",
            f"STATE {self.peers.local.cluster_state.value}",
            f"LEADER {self.peers.local.leader or '?CONFUSED'}",
        ]
        return " ".join(buffer_data).encode("utf-8")
