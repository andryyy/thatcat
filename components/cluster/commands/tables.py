import asyncio
import json
import base64

from ..exceptions import *
from .plugin import CommandPlugin
from components.database import *
from components.logs import logger
from components.models.cluster import CritErrors
from components.utils.cryptography import dict_digest_sha1


_modified_tables = dict()


class PatchTableCommand(CommandPlugin):
    name = "PATCHTABLE"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        global _modified_tables

        for k, v in _modified_tables.items():
            if v == table and k != lock_id:
                raise DocumentNotUpdated("Table is being modified by another lock")

        lock_id, table_w_hash, table_payload = data.payload.split(" ")
        table, table_digest = table_w_hash.split("@")

        async with TinyDB(**dbparams(lock_id)) as db:
            if not lock_id in _modified_tables:
                _modified_tables[lock_id] = set()

            _modified_tables[lock_id].add(table)

            try:
                table_data = {doc.doc_id: doc for doc in db.table(table).all()}
                errors = []
                local_table_digest = dict_digest_sha1(table_data)

                if local_table_digest != table_digest:
                    return CritErrors.TABLE_HASH_MISMATCH.response

                diff = json.loads(base64.b64decode(table_payload))

                for doc_id, docs in diff["changed"].items():
                    try:
                        a, b = docs
                        c = db.table(table).get(doc_id=doc_id)
                        assert c == a
                        db.table(table).upsert(Document(b, doc_id=doc_id))
                    except Exception as e:
                        raise DocumentNotUpdated(e)

                for doc_id, doc in diff["added"].items():
                    try:
                        db.table(table).insert(Document(doc, doc_id=doc_id))
                    except Exception as e:
                        raise DocumentNotInserted(e)

                for doc_id, doc in diff["removed"].items():
                    try:
                        a = db.table(table).get(doc_id=doc_id)
                        assert a == doc
                        db.table(table).remove(doc_ids=[int(doc_id)])
                    except Exception as e:
                        raise DocumentNotRemoved(e)

                return "ACK"

            except PatchException as e:
                logger.critical(f"Patching table failed: {str(e)}")
                return CritErrors.PATCH_EXCEPTION.response
            except Exception as e:
                logger.critical(f"Patching table failed for unhandled reason: {str(e)}")
                return CritErrors.PATCH_EXCEPTION.response


class FullTableCommand(CommandPlugin):
    name = "FULLTABLE"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        global _modified_tables
        lock_id, table_w_hash, table_payload = data.payload.split(" ")
        table, table_digest = table_w_hash.split("@")

        async with TinyDB(**dbparams(lock_id)) as db:
            if not lock_id in _modified_tables:
                _modified_tables[lock_id] = set()

            _modified_tables[lock_id].add(table)

            try:
                insert_data = json.loads(base64.b64decode(table_payload))
                db.table(table).truncate()
                for doc_id, doc in insert_data.items():
                    db.table(table).insert(Document(doc, doc_id=doc_id))

                return "ACK"

            except Exception as e:
                logger.critical(
                    f"Full table patch failed for unhandled reason: {str(e)}"
                )
                return CritErrors.CANNOT_APPLY.response


class CommitTableCommand(CommandPlugin):
    name = "COMMIT"

    async def handle(self, cluster: "Server", data: "IncomingData") -> str:
        global _modified_tables
        lock_id = data.payload

        if not lock_id in _modified_tables:
            return CritErrors.NOTHING_TO_COMMIT.response

        try:
            commit_tables = _modified_tables[lock_id]
            del _modified_tables[lock_id]

            await dbcommit(commit_tables, lock_id)

            for table in commit_tables:
                if table in STATE.query_cache.copy():
                    STATE.query_cache.pop(table, None)

            return "ACK"
        except Exception as e:
            logger.critical(f"Commit failed for unhandled reason: {str(e)}")
            return CritErrors.CANNOT_COMMIT.response
