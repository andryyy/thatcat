import asyncio
import ssl

from .exceptions import MonitoringTaskExists
from .leader import elect_leader
from components.logs import logger
from components.utils.datetimes import ntime_utc_now
from config import defaults
from contextlib import suppress


class Monitor:
    def __init__(self, cluster: "Server"):
        self.cluster = cluster

    async def server(self):
        c = 0

        while True:
            await asyncio.sleep(1)
            c += 1

            if not self.cluster.peers.local.cluster_complete:
                if (
                    self.cluster.peers._first_complete.is_set()
                    and self.cluster.peers.local.leader
                    and c % 4
                ):
                    continue

                try:
                    for peer, data in self.cluster.peers.remotes.items():
                        if not data.graceful_shutdown:
                            async with self.cluster.receiving:
                                sent = await self.cluster.send_command("STATUS", peer)
                                await self.cluster.await_receivers(
                                    sent, raise_err=False, timeout=3
                                )
                except Exception as e:
                    logger.critical(e)
                    pass
                finally:
                    elect_leader(self.cluster.peers)
            c = 0

    async def _cleanup_peer_connection(self, peer):
        logger.info(f"Removing peer {peer}")
        await self.cluster.peers.reset(peer)
        elect_leader(self.cluster.peers)

    def _get_task_index_by_name(self, name: str) -> int | None:
        for index, task in enumerate(self.cluster.tasks):
            if task.get_name() == name:
                return index
        return None

    async def peer_worker(self, name):
        ireader, iwriter = self.cluster.peers.remotes[name].streams.ingress
        timeout_c = 0
        c = 0

        logger.info(f"Evaluating stream for {name}")
        while not name in self.cluster.peers.get_established():
            await asyncio.sleep(0.125)

        oreader, owriter = self.cluster.peers.remotes[name].streams.egress

        elect_leader(self.cluster.peers)

        while True and timeout_c < 3:
            try:
                assert not all(
                    [
                        oreader.at_eof(),
                        ireader.at_eof(),
                        iwriter.is_closing(),
                        owriter.is_closing(),
                    ]
                )

                async with asyncio.timeout(defaults.CLUSTER_PEERS_TIMEOUT * 3):
                    iwriter.write(b"\x11")
                    await iwriter.drain()
                    res = await oreader.readexactly(1)
                    assert res == b"\x11"

                timeout_c = 0
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info(f"Stopping peer monitoring for {name}")
                break
            except TimeoutError:
                timeout_c += 1
                continue
            except (
                AssertionError,
                ConnectionResetError,
                asyncio.exceptions.IncompleteReadError,
            ):
                logger.error(f"Peer {name} failed")
                break

        try:
            iwriter.close()
            async with asyncio.timeout(0.1):
                await iwriter.wait_closed()
        except:
            pass

        try:
            owriter.close()
            async with asyncio.timeout(0.1):
                await owriter.wait_closed()
            await owriter.wait_closed()
        except:
            pass

    def _on_task_done(self, task: asyncio.Task):
        if not self.cluster.stop_event.is_set():
            sub_t = asyncio.create_task(self._cleanup_peer_connection(task.get_name()))
            self.cluster.tasks.add(sub_t)
            sub_t.add_done_callback(self.cluster.tasks.discard)

        self.cluster.tasks.discard(task)

    async def peer(self, name):
        if name in [task.get_name() for task in self.cluster.tasks]:
            raise MonitoringTaskExists(name)

        t = asyncio.create_task(self.peer_worker(name), name=name)
        self.cluster.tasks.add(t)
        t.add_done_callback(self._on_task_done)
