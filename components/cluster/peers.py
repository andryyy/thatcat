import asyncio, json, zlib, base64, socket

from components.cluster.ssl import get_ssl_context
from components.logs import logger
from components.models.cluster import ConnectionStatus, LocalPeer, RemotePeer, Role
from components.utils import ensure_list
from config import defaults
from ipaddress import IPv4Address, IPv6Address


class Peers:
    def __init__(self, cluster):
        self.remotes = dict()
        self.cluster = cluster

        for peer in defaults.CLUSTER_PEERS:
            remote_peer = RemotePeer(**peer)
            self.remotes[peer["name"]] = remote_peer

        self.local = LocalPeer(**defaults.CLUSTER_SELF)

    async def leader_election(self):
        def _destroy():
            self.local.leader = None
            self.local.role = Role.FOLLOWER
            self.local.cluster = ""
            self.local.cluster_complete.clear()

        n_eligible_peers = len(self.get_established(include_local=True))
        n_all_peers = len(self.remotes) + 1  # + self

        if not (n_eligible_peers >= (51 / 100) * n_all_peers):
            logger.warning("Not enough peers for election")
            _destroy()
            return

        leader, started = min(
            (
                (peer.meta.name, peer.meta.started)
                for peer in self.get_established(names_only=False)
            ),
            key=lambda x: x[1],
            default=(None, float("inf")),
        )

        if self.local.started < started:
            if self.local.leader != self.local.name:
                logger.info(
                    f"\033[35m\033[107mThis node ({self.local.name}) is leader\033[0m"
                )
                self.local.leader = self.local.name
                self.local.role = Role.LEADER
        else:
            if not self.remotes[leader].meta.leader:
                _destroy()
                logger.warning(
                    f"Potential leader {leader} is electing or confused, waiting"
                )
                return
            elif self.remotes[leader].meta.leader != leader:
                _destroy()
                logger.warning(
                    f"Potential leader {leader} reports a different leader; waiting"
                )
                return
            elif self.local.leader != leader:
                self.local.leader = leader
                self.local.role = Role.FOLLOWER
                logger.info(f"\033[35m\033[107m{leader} is leader\033[0m")

        if self.local.leader:
            self.local.cluster = ";".join(
                self.get_established(include_local=True, sorted_output=True)
            )
            for peer in self.remotes:
                if (
                    not self.remotes[peer].meta
                    or not self.remotes[peer].meta.cluster
                    or self.local.cluster != self.remotes[peer].meta.cluster
                ):
                    self.local.cluster_complete.clear()
                    break
            else:
                self.local.cluster_complete.set()
                if self.local.role == Role.LEADER:
                    if (
                        len(self.get_established(include_local=True))
                        == len(self.remotes) + 1
                    ):
                        await self._fan_out_db()

        logger.info(f"Cluster size {n_eligible_peers}/{n_all_peers}")

    async def _fan_out_db(self):
        from components.database import db

        payload = {
            "format": 2,
            "tables": {},
        }

        try:
            async with db:
                for table, doc_versions in db._manifest.get("tables", {}).items():
                    payload["tables"][table] = {
                        "docs": {},
                        "deleted_ids": [],
                        "doc_versions": doc_versions,
                    }
                    for doc_id in (
                        db._manifest["tables"][table].get("doc_versions", {}).keys()
                    ):
                        doc = await db.get(table, doc_id)
                        if doc:
                            payload["tables"][table]["docs"][doc_id] = doc
                        else:
                            payload["tables"][table]["doc_versions"].pop(doc_id, None)
                            payload["tables"][table]["deleted_ids"].append(doc_id)

                raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                b64 = base64.b64encode(zlib.compress(raw)).decode("ascii")

                await db.sync_in(b64)

                for peer in self.remotes:
                    await self.cluster.send_command(
                        f"DBSYNC BLOCK {b64}", peer, raise_err=True
                    )

        except Exception as e:
            logger.critical(e)

    async def disconnect(self, name: str, gracefully: bool = False) -> bool:
        for t in self.cluster.tasks:
            if t.get_name() == name:
                if gracefully:
                    self.remotes[name].graceful_shutdown = True
                t.cancel()
                await t
                break
        else:
            return False
        return True

    async def connect(self, name: str):
        async def _determine_ip():
            errors = dict()
            loop = asyncio.get_running_loop()
            for ip in [peer.ip4, peer.ip6]:
                if ip == peer.ip4:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                else:
                    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.setblocking(False)
                try:
                    await asyncio.wait_for(
                        loop.sock_connect(sock, (ip, peer.port)),
                        timeout=defaults.CLUSTER_PEERS_TIMEOUT / 2,
                    )
                    sock.close()
                    if errors:
                        return ip, (ConnectionStatus.OK_WITH_PREVIOUS_ERRORS, errors)
                    return ip, (ConnectionStatus.OK, {})
                except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as e:
                    errors[ip] = (ConnectionStatus.SOCKET_REFUSED, str(e))
                    sock.close()

            return None, (ConnectionStatus.ALL_AVAILABLE_FAILED, errors)

        peer = self.remotes[name]

        async with peer.lock:
            if peer.streams.egress:
                ereader, ewriter = peer.streams.egress
                if ereader.at_eof() or ewriter.is_closing():
                    logger.warning(f"Reconnecting egress streams to {name} after loss")
                    ewriter.close()
                    await ewriter.wait_closed()
                    peer.streams.egress = None

            if not peer.streams.egress:
                ip, status = await _determine_ip()
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

    def get_offline_peers(self):
        return [p for p in self.remotes if p not in self.get_established()]

    def get_established(
        self,
        names_only: bool = True,
        include_local: bool = False,
        sorted_output: bool = False,
    ):
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

    def get_peer_by_raddr(self, raddr) -> str:
        try:
            raddr = IPv6Address(raddr).exploded
            for peer in defaults.CLUSTER_PEERS:
                if peer.get("ip6") and IPv6Address(peer.get("ip6")).exploded == raddr:
                    return self.remotes[peer["name"]]
        except:
            for peer in defaults.CLUSTER_PEERS:
                if raddr in [
                    peer.get("ip4", ""),
                    peer.get("nat_ip4", ""),
                ]:
                    return self.remotes[peer["name"]]
