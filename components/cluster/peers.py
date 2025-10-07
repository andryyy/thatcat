"""Cluster peer management module for handling peer connections and leader election."""

import asyncio
import base64
import json
import socket
import zlib
from ipaddress import IPv4Address, IPv6Address

from components.cluster.ssl import get_ssl_context
from components.logs import logger
from components.models.cluster import ConnectionStatus, LocalPeer, RemotePeer, Role
from components.utils.misc import ensure_list
from config import defaults

# Constants
QUORUM_PERCENTAGE = 0.51  # 51% required for leader election
PREFERRED_IP_VERSION = 4  # Prefer IPv4 (4) or IPv6 (6)


class Peers:
    """
    Manage cluster peers, connections, and leader election.

    Handles peer connections, leader election via earliest-started algorithm,
    cluster membership tracking, and database synchronization.
    """

    def __init__(self, cluster):
        self.remotes = dict()
        self.cluster = cluster

        for peer in defaults.CLUSTER_PEERS:
            remote_peer = RemotePeer(**peer)
            self.remotes[peer["name"]] = remote_peer

        self.local = LocalPeer(**defaults.CLUSTER_SELF)

    def _reset_leader_state(self):
        """Reset local peer to non-leader state."""
        self.local.leader = None
        self.local.role = Role.FOLLOWER
        self.local.cluster = ""
        self.local.cluster_complete.clear()

    def _has_quorum(self) -> tuple[bool, int, int]:
        """Check if there are enough peers for leader election. Returns (has_quorum, eligible_count, total_count)."""
        n_eligible_peers = len(self.get_established(include_local=True))
        n_all_peers = len(self.remotes) + 1  # + self
        has_quorum = n_eligible_peers >= QUORUM_PERCENTAGE * n_all_peers
        return has_quorum, n_eligible_peers, n_all_peers

    def _elect_leader_from_peers(self) -> tuple[str | None, float]:
        """Elect leader based on earliest started timestamp. Returns (leader_name, started_timestamp)."""
        return min(
            (
                (peer.meta.name, peer.meta.started)
                for peer in self.get_established(names_only=False)
            ),
            key=lambda x: x[1],
            default=(None, float("inf")),
        )

    def _promote_to_leader(self):
        """Promote this node to leader."""
        if self.local.leader != self.local.name:
            logger.info(
                f"\033[35m\033[107mThis node ({self.local.name}) is leader\033[0m"
            )
            self.local.leader = self.local.name
            self.local.role = Role.LEADER

    def _validate_remote_leader(self, leader: str) -> bool:
        """Validate that the remote leader is consistent. Returns True if valid."""
        remote_leader = self.remotes[leader]

        if not remote_leader.meta.leader:
            self._reset_leader_state()
            logger.warning(
                f"Potential leader {leader} is electing or confused, waiting"
            )
            return False

        if remote_leader.meta.leader != leader:
            self._reset_leader_state()
            logger.warning(
                f"Potential leader {leader} reports a different leader; waiting"
            )
            return False

        return True

    def _set_follower_role(self, leader: str):
        """Set this node as a follower of the given leader."""
        if self.local.leader != leader:
            self.local.leader = leader
            self.local.role = Role.FOLLOWER
            logger.info(f"\033[35m\033[107m{leader} is leader\033[0m")

    def _update_cluster_membership(self):
        """Update cluster membership string and check if cluster is complete."""
        self.local.cluster = ";".join(
            self.get_established(include_local=True, sorted_output=True)
        )

        # Check if all peers have the same cluster view
        for peer_name in self.remotes:
            peer = self.remotes[peer_name]
            if (
                not peer.meta
                or not peer.meta.cluster
                or self.local.cluster != peer.meta.cluster
            ):
                self.local.cluster_complete.clear()
                return False

        self.local.cluster_complete.set()
        return True

    async def leader_election(self):
        """Perform leader election based on earliest started timestamp and quorum."""
        has_quorum, n_eligible_peers, n_all_peers = self._has_quorum()

        if not has_quorum:
            logger.warning("Not enough peers for election")
            self._reset_leader_state()
            return

        leader, started = self._elect_leader_from_peers()

        if self.local.started < started:
            self._promote_to_leader()
        else:
            if not self._validate_remote_leader(leader):
                return
            self._set_follower_role(leader)

        if self.local.leader:
            cluster_complete = self._update_cluster_membership()

            if cluster_complete and self.local.role == Role.LEADER:
                if (
                    len(self.get_established(include_local=True))
                    == len(self.remotes) + 1
                ):
                    await self._fan_out_db()

        logger.info(f"Cluster size {n_eligible_peers}/{n_all_peers}")

    async def _build_db_sync_payload(self, db, payload_format_version: int):
        """Build database synchronization payload with all tables and documents."""
        payload = {
            "format": payload_format_version,
            "tables": {},
        }

        for table, doc_versions in db._manifest.get("tables", {}).items():
            payload["tables"][table] = {
                "docs": {},
                "deleted_ids": [],
                "doc_versions": doc_versions,
            }

            for doc_id in db._manifest["tables"][table].get("doc_versions", {}).keys():
                doc = await db.get(table, doc_id)
                if doc:
                    payload["tables"][table]["docs"][doc_id] = doc
                else:
                    payload["tables"][table]["doc_versions"].pop(doc_id, None)
                    payload["tables"][table]["deleted_ids"].append(doc_id)

        return payload

    async def _fan_out_db(self):
        """Synchronize database to all peers when leader and cluster is complete."""
        from components.database import db
        from components.database.database import SYNC_PAYLOAD_FORMAT_VERSION

        try:
            async with db:
                payload = await self._build_db_sync_payload(
                    db, SYNC_PAYLOAD_FORMAT_VERSION
                )

                raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                b64 = base64.b64encode(zlib.compress(raw)).decode("ascii")

                await db.sync_in(b64)

                for peer in self.remotes:
                    await self.cluster.send_command(
                        f"DBSYNC BLOCK {b64}", peer, raise_err=True
                    )

        except Exception as e:
            logger.critical(f"Database fan-out failed: {e}")

    async def disconnect(self, name: str, gracefully: bool = False) -> bool:
        """
        Disconnect from a peer by cancelling its watchdog task.

        This triggers cleanup in the watchdog task which will:
        - Close both ingress and egress stream writers
        - Clear the peer's streams and metadata
        - Trigger leader re-election

        Args:
            name: Name of the peer to disconnect from
            gracefully: If True, marks peer for graceful shutdown to prevent reconnection

        Returns:
            True if peer watchdog task was found and cancelled, False otherwise
        """
        if name not in self.remotes:
            logger.warning(f"Cannot disconnect unknown peer {name}")
            return False

        peer = self.remotes[name]

        # Set graceful shutdown flag to prevent reconnection attempts
        if gracefully:
            peer.graceful_shutdown = True

        # Find and cancel the watchdog task for this peer
        for task in self.cluster.tasks:
            if task.get_name() == name:
                logger.info(f"Disconnecting from {name} (graceful={gracefully})")
                task.cancel()

                # Wait for the task to complete its cleanup
                # The watchdog task handles closing both egress and ingress streams
                try:
                    await task
                except asyncio.CancelledError:
                    # Expected - task was cancelled
                    pass

                return True

        # No watchdog task found - peer may not be fully connected yet
        logger.debug(f"No watchdog task found for {name}, may not be connected")
        return False

    async def _test_single_ip(
        self, ip: str, port: int
    ) -> tuple[str, bool, Exception | None]:
        """Test connection to a single IP. Returns (ip, success, error)."""
        if ":" in ip:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        sock.setblocking(False)
        loop = asyncio.get_running_loop()

        try:
            await asyncio.wait_for(
                loop.sock_connect(sock, (ip, port)),
                timeout=defaults.CLUSTER_PEERS_TIMEOUT / 2,
            )
            sock.close()
            return ip, True, None
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
            sock.close()
            return ip, False, e

    async def _determine_best_ip(self, peer) -> tuple[str | None, tuple]:
        """Determine the best IP to connect to by testing all IPs concurrently."""
        # Build IP list with preferred version first
        available_ips = []
        if PREFERRED_IP_VERSION == 4:
            if peer.ip4:
                available_ips.append(peer.ip4)
            if peer.ip6:
                available_ips.append(peer.ip6)
        else:
            if peer.ip6:
                available_ips.append(peer.ip6)
            if peer.ip4:
                available_ips.append(peer.ip4)

        if not available_ips:
            return None, (ConnectionStatus.ALL_AVAILABLE_FAILED, {})

        # Test all IPs concurrently
        tasks = [self._test_single_ip(ip, peer.port) for ip in available_ips]
        results = await asyncio.gather(*tasks)

        errors = {}
        successful_ip = None

        # Find first successful IP (preserving preference order), collect all errors
        for ip, success, error in results:
            if success and not successful_ip:
                successful_ip = ip
            elif not success:
                errors[ip] = (ConnectionStatus.SOCKET_REFUSED, str(error))

        if successful_ip:
            if errors:
                return successful_ip, (ConnectionStatus.OK_WITH_PREVIOUS_ERRORS, errors)
            return successful_ip, (ConnectionStatus.OK, {})

        return None, (ConnectionStatus.ALL_AVAILABLE_FAILED, errors)

    async def _reconnect_if_needed(self, peer, name: str):
        """Check if egress streams need reconnection and reconnect if necessary."""
        if peer.streams.egress:
            ereader, ewriter = peer.streams.egress
            if ereader.at_eof() or ewriter.is_closing():
                logger.warning(f"Reconnecting egress streams to {name} after loss")
                ewriter.close()
                await ewriter.wait_closed()
                peer.streams.egress = None

    async def connect(self, name: str):
        """Connect to a peer by name. Returns (streams, status) tuple."""
        peer = self.remotes[name]

        async with peer.lock:
            await self._reconnect_if_needed(peer, name)

            if not peer.streams.egress:
                ip, status = await self._determine_best_ip(peer)
                if not ip:
                    return None, status
                try:
                    peer.streams.egress = await asyncio.open_connection(
                        ip, peer.port, ssl=get_ssl_context("client")
                    )
                    peer.graceful_shutdown = False
                except ConnectionRefusedError as e:
                    return None, (ConnectionStatus.REFUSED, e)

            return peer.streams.egress, (ConnectionStatus.CONNECTED, None)

    def get_offline_peers(self) -> list[str]:
        """Get list of peer names that are not currently established."""
        return [p for p in self.remotes if p not in self.get_established()]

    def get_established(
        self,
        names_only: bool = True,
        include_local: bool = False,
        sorted_output: bool = False,
    ) -> list[str] | list[RemotePeer | LocalPeer]:
        """
        Get list of established peers.

        Args:
            names_only: Return peer names (str) instead of peer objects
            include_local: Include local peer in the list
            sorted_output: Sort the output list

        Returns:
            List of peer names (if names_only=True) or peer objects
        """
        peers = []
        for peer, peer_data in self.remotes.items():
            if peer_data.established:
                if names_only:
                    peers.append(peer)
                else:
                    peers.append(peer_data)

        if include_local:
            if names_only:
                peers.append(self.local.name)
            else:
                peers.append(self.local)

        if names_only:
            if sorted_output:
                return sorted(peers)
            return peers

        if sorted_output:
            return sorted(peers, key=lambda peer: peer.name)
        return peers

    def get_peer_by_raddr(self, raddr) -> RemotePeer | None:
        """Get peer by remote address (IPv4 or IPv6). Returns RemotePeer or None."""
        # Try IPv6 first
        try:
            raddr_exploded = IPv6Address(raddr).exploded
            for peer in defaults.CLUSTER_PEERS:
                peer_ip6 = peer.get("ip6")
                if peer_ip6 and IPv6Address(peer_ip6).exploded == raddr_exploded:
                    return self.remotes[peer["name"]]
        except (ValueError, AttributeError):
            # Not a valid IPv6 address, try IPv4
            pass

        # Try IPv4 (including NAT)
        for peer in defaults.CLUSTER_PEERS:
            if raddr in [peer.get("ip4", ""), peer.get("nat_ip4", "")]:
                return self.remotes[peer["name"]]

        return None
