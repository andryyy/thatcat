import asyncio
import socket

from config import defaults
from components.cluster.ssl import get_ssl_context
from components.logs import logger
from components.models.cluster import (
    LocalPeer,
    RemotePeer,
    ConnectionStatus,
    IPvAnyAddress,
)
from components.utils import ensure_list


class Peers:
    def __init__(self):
        self.remotes = dict()

        for peer in defaults.CLUSTER_PEERS:
            peer = RemotePeer(**peer)
            self.remotes[peer.name] = peer

        self.local = LocalPeer(**defaults.CLUSTER_SELF)

    async def reset(self, name: str):
        if not name in self.remotes:
            raise AttributeError("Unknown peer")

        async with self.remotes[name].lock:
            self.remotes[name].streams.ingress = None
            self.remotes[name].streams.egress = None
            self.remotes[name].leader = None
            self.remotes[name].started = None
            self.remotes[name].cluster = ""

    async def connect(self, name: str):
        async def _determine_ip():
            errors = dict()
            peer_ips = [ip for ip in [peer.ip4, peer.ip6] if ip is not None]
            loop = asyncio.get_running_loop()
            for ip in peer_ips:
                if ip.version == 4:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                elif ip.version == 6:
                    sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.setblocking(False)
                try:
                    await asyncio.wait_for(
                        loop.sock_connect(sock, (str(ip), peer.port)),
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

        if not name in self.remotes:
            raise AttributeError("Unknown peer")

        peer = self.remotes[name]

        if not peer.streams.egress:
            ip, status = await _determine_ip()
            if not ip:
                return None, status
            try:
                peer.streams.egress = await asyncio.open_connection(
                    str(ip), peer.port, ssl=get_ssl_context("client")
                )
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
            if peer_data.healthy == True:
                if names_only:
                    peers.append(peer_data.name)
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
