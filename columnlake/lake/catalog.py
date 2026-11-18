from dataclasses import dataclass
import hashlib
import json
import sqlite3
from columnlake.source.events import DataError, exact_int
from columnlake.source.identity import RequestIdentity, canonical_json
from columnlake.source.session import StreamState
from columnlake.assembly.audit import BatchAudit
from columnlake.lake.storage import StorageError


@dataclass(frozen=True)
class BatchRecord:
    batch_no: int
    stream_id: str
    batch_id: str
    segment_id: int
    audit: BatchAudit


@dataclass(frozen=True)
class IntakeReceipt:
    request_id: str
    stream_id: str
    snapshot_id: int
    batch_numbers: tuple

    def to_obj(self):
        return {'request_id': self.request_id, 'stream_id': self.stream_id,
                'snapshot_id': self.snapshot_id, 'batch_numbers': list(self.batch_numbers)}

    @classmethod
    def from_obj(cls, value):
        keys = {'request_id', 'stream_id', 'snapshot_id', 'batch_numbers'}
        if type(value) is not dict or set(value) != keys:
            raise DataError('invalid receipt')
        if any(type(value[k]) is not str or not value[k] for k in ('request_id', 'stream_id')):
            raise DataError('invalid receipt identity')
        numbers = value['batch_numbers']
        if (not exact_int(value['snapshot_id'], 1) or type(numbers) is not list
                or any(not exact_int(n, 1) for n in numbers) or len(set(numbers)) != len(numbers)):
            raise DataError('invalid receipt members')
        return cls(value['request_id'], value['stream_id'], value['snapshot_id'], tuple(numbers))


def batch_record(row):
    try:
        return BatchRecord(row['batch_no'], row['stream_id'], row['batch_id'],
                           row['segment_id'], BatchAudit.from_obj(json.loads(row['audit'])))
    except (DataError, ValueError, TypeError, KeyError) as exc:
        raise StorageError('damaged batch directory') from exc


class LakeCatalog:
    def __init__(self, store):
        self.store = store

    def load_stream_state(self, stream_id):
        try:
            row = self.store.conn.execute('SELECT state FROM streams WHERE stream_id=?', (stream_id,)).fetchone()
            if row is None:
                return StreamState.empty(stream_id)
            state = StreamState.from_obj(json.loads(row[0]))
            if state.stream_id != stream_id:
                raise DataError('stored stream identity mismatch')
            ids = tuple(r[0] for r in self.store.conn.execute(
                'SELECT batch_id FROM batches WHERE stream_id=? ORDER BY batch_no', (stream_id,)))
            if ids != state.batch_ids:
                raise DataError('stream batch directory mismatch')
            return state
        except (sqlite3.Error, DataError, ValueError, TypeError, KeyError, RecursionError) as exc:
            raise StorageError('damaged stream state') from exc

    def save_stream_state(self, state):
        self.store.conn.execute(
            'INSERT INTO streams(stream_id,state) VALUES (?,?) ON CONFLICT(stream_id) DO UPDATE SET state=excluded.state',
            (state.stream_id, canonical_json(state.to_obj())))

    def record_batches(self, batches, segment_ids):
        numbers = []
        for batch, segment_id in zip(batches, segment_ids, strict=True):
            cursor = self.store.conn.execute(
                'INSERT INTO batches(stream_id,batch_id,segment_id,audit) VALUES (?,?,?,?)',
                (batch.stream_id, batch.batch_id, segment_id, canonical_json(batch.audit.to_obj())))
            numbers.append(cursor.lastrowid)
        return tuple(numbers)

    def find_receipt(self, request_id):
        try:
            row = self.store.conn.execute('SELECT * FROM receipts WHERE request_id=?', (request_id,)).fetchone()
            if row is None:
                return None
            identity = RequestIdentity(row['content'], row['digest'])
            if hashlib.sha256(identity.content.encode('utf-8')).hexdigest() != identity.digest:
                raise DataError('damaged request identity')
            receipt = IntakeReceipt.from_obj(json.loads(row['receipt']))
            if receipt.request_id != request_id or receipt.snapshot_id != row['snapshot_id']:
                raise DataError('receipt directory mismatch')
            content = json.loads(identity.content)
            if content['stream_id'] != receipt.stream_id:
                raise DataError('receipt stream mismatch')
            actual = tuple(r[0] for r in self.store.conn.execute(
                'SELECT batch_no FROM members WHERE snapshot_id=? AND batch_no NOT IN '
                '(SELECT batch_no FROM members WHERE snapshot_id=?) ORDER BY position',
                (receipt.snapshot_id, receipt.snapshot_id - 1)))
            if actual != receipt.batch_numbers:
                raise DataError('receipt batch membership mismatch')
            return identity, receipt
        except (sqlite3.Error, DataError, ValueError, TypeError, KeyError) as exc:
            raise StorageError('damaged intake receipt') from exc

    def write_receipt(self, identity, receipt):
        self.store.conn.execute('INSERT INTO receipts VALUES (?,?,?,?,?)',
                                (receipt.request_id, identity.content, identity.digest,
                                 receipt.snapshot_id, canonical_json(receipt.to_obj())))
