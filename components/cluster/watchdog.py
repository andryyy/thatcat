import asyncio

from components.logs import logger
from .models import Role, ErrorMessages


class Watchdog:
    def __init__(self, cluster: "Server"):  # noqa: F821
        self.cluster = cluster

    async def server(self):
        from components.database import db

        db_sync_required = False
        self.cluster.peers.leader_election()
        while not self.cluster.shutdown_trigger.is_set():
            if not self.cluster.peers.peers_consistent():
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
                    self.cluster.peers.leader_election()
                    if self.cluster.peers.local.role == Role.FOLLOWER:
                        db_sync_required = True

            elif db_sync_required:
                ret, resp = await self.cluster.send_command(
                    "DBSYNCREQ",
                    self.cluster.peers.local.leader,
                    raise_err=False,
                )
                if ret:
                    async with db:
                        await db.sync_in(resp[self.cluster.peers.local.leader])
                    db_sync_required = False
                else:
                    if resp[self.cluster.peers.local.leader] == ErrorMessages.NOT_READY:
                        logger.warning("Leader is not ready; retrying")
                    else:
                        logger.error("Could not request database from leader")

            await asyncio.sleep(0.8)

    async def _peer_worker(self, peer):
        failures = 0
        while failures < 5:
            try:
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
                logger.info(f"Watchdog of {peer.name} was cancelled")
                raise
            except TimeoutError:
                failures += 1
                logger.warning(f"{peer.name} is failing [{'#' * failures: <5}]")
                continue
            except (
                AssertionError,
                ConnectionResetError,
                BrokenPipeError,
                ConnectionAbortedError,
                asyncio.exceptions.IncompleteReadError,
            ) as e:
                logger.error(f"{peer.name} failed: {e}")
                break

        await self.cluster.peers.disconnect(peer.name)

    async def peer(self, peer):
        logger.info(f"Monitoring {peer.name}")
        self.cluster.peers.leader_election()
        t = asyncio.create_task(self._peer_worker(peer), name=peer.name)
        self.cluster.tasks.add(t)
        t.add_done_callback(self.cluster.tasks.discard)
