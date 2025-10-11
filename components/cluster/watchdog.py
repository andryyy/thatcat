import asyncio
import ssl

from components.logs import logger


class Watchdog:
    def __init__(self, cluster: "Server"):
        self.cluster = cluster

    async def server(self):
        while not self.cluster.shutdown_trigger.is_set():
            await asyncio.sleep(1)
            if not self.cluster.peers.local.cluster_complete.is_set():
                try:
                    for peer, data in self.cluster.peers.remotes.items():
                        if data.established:
                            await self.cluster.send_command(
                                "STATUS", peer, raise_err=False
                            )
                        elif not data.graceful_shutdown:
                            await self.cluster.send_command("INIT", peer)
                except Exception as e:
                    logger.warning(e)
                finally:
                    await self.cluster.peers.leader_election()

    async def _peer_worker(self, name):
        failures = 0
        while failures < 5:
            try:
                peer = self.cluster.peers.remotes[name]
                ireader, iwriter = peer.streams.ingress
                ereader, ewriter = peer.streams.egress

                assert not all(
                    [
                        ereader.at_eof(),
                        ireader.at_eof(),
                        iwriter.is_closing(),
                        ewriter.is_closing(),
                    ]
                )

                async with asyncio.timeout(1.5):
                    iwriter.write(b"\x00")
                    await iwriter.drain()
                    res = await ereader.readexactly(1)
                    assert res == b"\x00"

                failures = 0
                await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                logger.info(f"Stopping monitoring of {name}")
                if self.cluster.shutdown_trigger.is_set():
                    raise
                break
            except TimeoutError:
                failures += 1
                logger.warning(f"{name} is failing [{'#' * failures: <5}]")
                continue
            except (
                AssertionError,
                ConnectionResetError,
                BrokenPipeError,
                ConnectionAbortedError,
                asyncio.exceptions.IncompleteReadError,
            ):
                logger.error(f"{name} failed")
                break

        try:
            iwriter.close()
            async with asyncio.timeout(0.1):
                await iwriter.wait_closed()
        except:
            pass

        try:
            ewriter.close()
            async with asyncio.timeout(0.1):
                await ewriter.wait_closed()
            await ewriter.wait_closed()
        except:
            pass

        logger.info(f"Removing {name}")
        async with peer.lock:
            peer.streams.ingress = None
            peer.streams.egress = None
            peer.meta = None
            await self.cluster.peers.leader_election()

    async def peer(self, name):
        logger.info(f"Monitoring {name}")
        await self.cluster.peers.leader_election()
        t = asyncio.create_task(self._peer_worker(name), name=name)
        self.cluster.tasks.add(t)
        t.add_done_callback(self.cluster.tasks.discard)
