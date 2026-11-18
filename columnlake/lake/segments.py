from dataclasses import dataclass
import hashlib
import json
import sqlite3
from columnlake.source.events import DataError
from columnlake.source.identity import canonical_json
from columnlake.assembly.audit import BatchAudit, verify_audit
from columnlake.lake.storage import StorageError
from columnlake.lake.encoding import EncodedBatch, encode_batch, decode_batch


@dataclass(frozen=True)
class SegmentRecord:
    segment_id: int
    encoded: EncodedBatch
    schema: object
    rows: object
    audit: BatchAudit


def write_segment(store, batch):
    encoded = encode_batch(batch)
    data = encoded.payload.encode('utf-8')
    checksum = hashlib.sha256(data).hexdigest()
    cursor = store.conn.execute(
        'INSERT INTO segments(payload,checksum,byte_length,audit) VALUES (?,?,?,?)',
        (encoded.payload, checksum, len(data), canonical_json(batch.audit.to_obj())))
    return cursor.lastrowid


def read_segment(store, segment_id, expected_audit=None):
    try:
        row = store.conn.execute('SELECT * FROM segments WHERE segment_id=?', (segment_id,)).fetchone()
        if row is None or type(row['payload']) is not str:
            raise StorageError('missing segment payload')
        data = row['payload'].encode('utf-8')
        if len(data) != row['byte_length'] or hashlib.sha256(data).hexdigest() != row['checksum']:
            raise StorageError('segment payload checksum mismatch')
        audit = BatchAudit.from_obj(json.loads(row['audit']))
        if expected_audit is not None and expected_audit != audit:
            raise StorageError('segment/catalog audit mismatch')
        encoded = EncodedBatch(row['payload'])
        schema, rows = decode_batch(encoded)
        verify_audit(schema, rows, audit)
        return SegmentRecord(segment_id, encoded, schema, rows, audit)
    except StorageError:
        raise
    except (sqlite3.Error, DataError, ValueError, TypeError, KeyError, IndexError, RecursionError) as exc:
        raise StorageError('damaged segment encoding or audit') from exc
