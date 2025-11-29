import asyncio
import socket

from .ssl import get_ssl_context
from .models import ClusterState, ConnectionStatus, LocalPeer, RemotePeer, Role
from components.logs import logger
from config import defaults
from ipaddress import IPv6Address

QUORUM_PERCENTAGE = 0.51  # 51% required for leader election
if defaults.DISABLE_CLUSTER_QUORUM:
  QUORUM_PERCENTAGE = 0

PREFERRED_IP_VERSION = 4  # Prefer IPv4 (4) or IPv6 (6)


class Peers:
    def __init__(self, cluster):
        self.remotes = dict()
        self.cluster = cluster

        for peer in defaults.CLUSTER_PEERS:
            remote_peer = RemotePeer(**defaults.CLUSTER_PEERS[peer] | {"name": peer})
            self.remotes[peer] = remote_peer

        self.local = LocalPeer(**defaults.CLUSTER_SELF)

    def _reset_state(self, state: ClusterState = ClusterState.NONE):
        self.local.leader = None
        self.local.cluster = None
        self.local.role = Role.FOLLOWER
        self.local.cluster_state = state
        self.cluster.locks = dict()

    def leader_election(self):
        eligible = self.get_established(include_local=True, sorted_output=True)
        n_eligible = len(eligible)
        n_peers = len(self.remotes) + 1  # + self
        has_quorum = n_eligible >= QUORUM_PERCENTAGE * n_peers
        self.local.cluster_state = ClusterState.ELECTING

        if not has_quorum:
            logger.warning(f"Not enough peers for election {n_eligible}/{n_peers}")
            return self._reset_state(ClusterState.NO_QUORUM)

        name, started = min(
            (
                (peer.meta.name, peer.meta.started)
                for peer in self.get_established(names_only=False)
            ),
            key=lambda x: x[1],
            default=(None, float("inf")),
        )
        if self.local.started < started:
            self.local.leader = self.local.name
            self.local.role = Role.LEADER
        else:
            self.local.leader = name
            self.local.role = Role.FOLLOWER

        self.local.cluster = ";".join(eligible)

        for peer in self.get_established(names_only=False):
            if not peer.meta.leader:
                logger.warning(f"{peer.name} has not yet elected a leader; waiting")
                break
            elif peer.meta.leader != self.local.leader:
                logger.warning(f"{peer.name} reports a different leader; waiting")
                break
            elif peer.meta.cluster != self.local.cluster:
                logger.warning(
                    f"{peer.name} reports inconsistent cluster node; waiting"
                )
                break
        else:
            logger.info(f"\033[35m\033[107m{self.local.leader} is leader\033[0m")
            logger.info(f"\033[35m\033[107mCluster size {n_eligible}/{n_peers}\033[0m")
            if n_eligible == n_peers:
                self.local.cluster_state = ClusterState.COMPLETE
            else:
                self.local.cluster_state = ClusterState.CONSISTENT_WITH_MISSING

    async def disconnect(self, name: str, gracefully: bool = False) -> bool:
        if name not in self.remotes:
            logger.warning(f"Cannot disconnect unknown peer {name}")
            return False

        logger.info(f"Disconnecting {name} (graceful={gracefully})")
        peer = self.remotes[name]

        async with peer.lock:
            if gracefully:
                peer.graceful_shutdown = True
            for task in self.cluster.tasks:
                if task.get_name() == name:
                    logger.info(f"Requesting cancellation of {name}'s watchdog")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    break

            if peer.streams.ingress:
                _, iwriter = peer.streams.ingress
                try:
                    iwriter.close()
                    async with asyncio.timeout(0.1):
                        await iwriter.wait_closed()
                except:  # noqa: E722
                    pass

            if peer.streams.egress:
                _, ewriter = peer.streams.egress
                try:
                    ewriter.close()
                    async with asyncio.timeout(0.1):
                        await ewriter.wait_closed()
                    await ewriter.wait_closed()
                except:  # noqa: E722
                    pass

            peer.streams.ingress = None
            peer.streams.egress = None
            peer.meta = None
            self._reset_state(ClusterState.INCONSISTENT)
            self.leader_election()

    async def _test_single_ip(
        self, ip: str, port: int
    ) -> tuple[str, bool, Exception | None]:
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

    async def connect(self, name: str):
        peer = self.remotes[name]

        async with peer.lock:
            if peer.streams.egress:
                ereader, ewriter = peer.streams.egress
                if ereader.at_eof() or ewriter.is_closing():
                    logger.warning(
                        f"Reconnecting egress streams to {peer.name} after loss"
                    )
                    ewriter.close()
                    await ewriter.wait_closed()
                    peer.streams.egress = None

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
                except ConnectionResetError as e:
                    return None, (ConnectionStatus.RESET, e)
                except Exception as e:
                    return None, (ConnectionStatus.UNKNOWN_ERROR, e)

            return peer.streams.egress, (ConnectionStatus.CONNECTED, None)

    def get_offline_peers(self) -> list[str]:
        return [p for p in self.remotes if p not in self.get_established()]

    def peers_consistent(self) -> bool:
        if not self.get_established():
            return False
        for peer in self.get_established(names_only=False):
            if not peer.meta:
                return False
            if peer.meta.state not in {
                ClusterState.COMPLETE,
                ClusterState.CONSISTENT_WITH_MISSING,
            }:
                return False
            if not self.local.cluster_state == peer.meta.state:
                return False
            if peer.meta.leader != self.local.leader:
                return False
            if peer.meta.cluster != self.local.cluster:
                return False
        return True

    def get_established(
        self,
        names_only: bool = True,
        include_local: bool = False,
        sorted_output: bool = False,
    ) -> list[str] | list[RemotePeer | LocalPeer]:
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
        try:
            raddr_exploded = IPv6Address(raddr).exploded
            for peer in defaults.CLUSTER_PEERS:
                peer_ip6 = defaults.CLUSTER_PEERS[peer].get("ip6")
                if peer_ip6 and IPv6Address(peer_ip6).exploded == raddr_exploded:
                    return self.remotes[peer]
        except (ValueError, AttributeError):
            pass

        for peer in defaults.CLUSTER_PEERS:
            if raddr in [
                defaults.CLUSTER_PEERS[peer].get("ip4", ""),
                defaults.CLUSTER_PEERS[peer].get("nat_ip4", ""),
            ]:
                return self.remotes[peer]

        return None
