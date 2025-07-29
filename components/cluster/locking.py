import asyncio
import base64
import json
import os

from . import cluster
from .exceptions import LockException, PatchException
from components.database import *
from components.models.cluster import CritErrors, ValidationError
from components.logs import logger
from components.utils import ensure_list
from components.utils.cryptography import dict_digest_sha1


class ClusterLock:
    def __init__(self, tables: list | str):
        self.tables = ensure_list(tables)
        self.data_before = dict()
        self._ctx_vars = dict()
        for table in self.tables:
            if table in IN_MEMORY_DB["CACHE"]["FORMS"].copy():
                IN_MEMORY_DB["CACHE"]["FORMS"].pop(table, {})

    @staticmethod
    def compare_tables(d1, d2):
        keys1 = set(d1.keys())
        keys2 = set(d2.keys())

        added = keys2 - keys1
        removed = keys1 - keys2
        common_keys = keys1 & keys2
        changed = {
            doc_id: (d1[doc_id], d2[doc_id])
            for doc_id in common_keys
            if d1[doc_id] != d2[doc_id]
        }

        if not changed and not added and not removed:
            return None

        return {
            "changed": changed,
            "added": {doc_id: d2[doc_id] for doc_id in added},
            "removed": {doc_id: d1[doc_id] for doc_id in removed},
        }

    async def __aenter__(self):
        self.leader_data = cluster.peers.remotes[cluster.peers.local.leader]
        lock_id = await cluster.acquire_lock(self.tables)
        self._ctx_vars[lock_id] = CTX_LOCK_ID.set(lock_id)
        self.db_params = dbparams()

        async with TinyDB(**self.db_params) as db:
            for t in self.tables:
                self.data_before[t] = dict()
                self.data_before[t]["data"] = {
                    doc.doc_id: doc for doc in db.table(t).all()
                }
                self.data_before[t]["digest"] = dict_digest_sha1(
                    self.data_before[t]["data"]
                )

        return cluster

    async def __aexit__(self, exc_type, exc, tb):
        lock_id = CTX_LOCK_ID.get()
        if exc:
            await cluster.release(lock_id, self.tables)
            if isinstance(exc, ValidationError) or isinstance(exc, ValueError):
                raise exc
            logger.critical(exc)
            raise LockException(exc)

        async with TinyDB(**self.db_params) as db:
            commit = False

            try:
                for t in self.tables:
                    table_data = {doc.doc_id: doc for doc in db.table(t).all()}
                    diff = self.compare_tables(self.data_before[t]["data"], table_data)
                    if diff:
                        commit = True
                        diff_json_bytes = json.dumps(diff).encode("utf-8")
                        patchtable_data = base64.b64encode(diff_json_bytes).decode(
                            "utf-8"
                        )

                        async with cluster.receiving:
                            patch_peers = cluster.peers.get_established()
                            failed_peers = set()
                            for peer in patch_peers:
                                sent = await cluster.send_command(
                                    "PATCHTABLE {lock} {table}@{digest} {data}".format(
                                        lock=lock_id,
                                        table=t,
                                        digest=self.data_before[t]["digest"],
                                        data=patchtable_data,
                                    ),
                                    peer,
                                )
                                result, response = await cluster.await_receivers(
                                    sent, raise_err=False
                                )
                                if not result:
                                    if (
                                        response.get(peer)
                                        == CritErrors.TABLE_HASH_MISMATCH
                                    ):
                                        failed_peers.add(peer)
                                    else:
                                        raise PatchException(response.get(peer))

                            if failed_peers:
                                n_all_peers = len(patch_peers) + 1
                                n_successful_peers = n_all_peers - len(failed_peers)
                                if n_successful_peers >= (51 / 100) * n_all_peers:
                                    table_json_bytes = json.dumps(
                                        table_data, sort_keys=True
                                    ).encode("utf-8")

                                    fulltable_data = base64.b64encode(
                                        table_json_bytes
                                    ).decode("utf-8")
                                    for peer in failed_peers:
                                        sent = await cluster.send_command(
                                            "FULLTABLE {lock} {table}@{digest} {data}".format(
                                                lock=lock_id,
                                                table=t,
                                                digest=self.data_before[t]["digest"],
                                                data=fulltable_data,
                                            ),
                                            peer,
                                        )

                                        sent = await cluster.send_command(
                                            f"{apply_mode} {lock_id} {t}@{self.data_before[t]['digest']} {apply_data}",
                                            peer,
                                        )
                                        (
                                            result,
                                            response,
                                        ) = await cluster.await_receivers(
                                            sent, raise_err=True
                                        )
                                else:
                                    raise PatchException(response[peer])

                if commit:
                    if (
                        not self.leader_data
                        == cluster.peers.remotes[cluster.peers.local.leader]
                    ):
                        raise LockException("Leader changed while locked")

                    async with cluster.receiving:
                        sent = await cluster.send_command(f"COMMIT {lock_id}", "*")
                        await cluster.await_receivers(sent, raise_err=True)
                    await dbcommit(self.tables)
                else:
                    os.unlink(self.db_params["filename"])

            except Exception as e:
                logger.critical(e)
                raise LockException(e)
            finally:
                await cluster.release(lock_id, self.tables)
                CTX_LOCK_ID.reset(self._ctx_vars[lock_id])
