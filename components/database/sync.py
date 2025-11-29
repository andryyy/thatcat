import base64
import json
import zlib

from . import db
from .database import SYNC_PAYLOAD_FORMAT_VERSION


async def generate_full_sync_payload():
    payload = {
        "format": SYNC_PAYLOAD_FORMAT_VERSION,
        "tables": {},
    }

    async with db:
        for table, table_dict in db._manifest.get("tables", {}).items():
            payload["tables"][table] = {
                "docs": {},
                "deleted_ids": [],
                "doc_versions": table_dict.get("doc_versions", {}),
            }

            for doc_id in list(
                db._manifest["tables"][table].get("doc_versions", {}).keys()
            ):
                doc = await db.get(table, doc_id)
                if doc:
                    payload["tables"][table]["docs"][doc_id] = doc
                else:
                    payload["tables"][table]["doc_versions"].pop(doc_id, None)
                    payload["tables"][table]["deleted_ids"].append(doc_id)

        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return base64.b64encode(zlib.compress(raw))
