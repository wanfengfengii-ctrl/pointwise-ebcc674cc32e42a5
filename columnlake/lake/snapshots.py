from dataclasses import dataclass
import sqlite3
from columnlake.source.events import DataError, exact_int
from columnlake.lake.storage import StorageError
from columnlake.lake.catalog import batch_record


@dataclass(frozen=True)
class SnapshotRecord:
    snapshot_id: int
    members: tuple


def validate_members(store, snapshot_id, member_count):
    try:
        rows = list(store.conn.execute(
            'SELECT m.position,b.* FROM members m LEFT JOIN batches b ON b.batch_no=m.batch_no '
            'WHERE m.snapshot_id=? ORDER BY m.position', (snapshot_id,)))
        if len(rows) != member_count or [r['position'] for r in rows] != list(range(len(rows))):
            raise StorageError('snapshot member count or order mismatch')
        if any(row['batch_no'] is None for row in rows):
            raise StorageError('snapshot references a missing batch')
        records = tuple(batch_record(row) for row in rows)
        if len({r.batch_no for r in records}) != len(records):
            raise StorageError('duplicate snapshot member')
        return records
    except sqlite3.Error as exc:
        raise StorageError('damaged snapshot directory') from exc


def load_snapshot(store, snapshot_id):
    if not exact_int(snapshot_id):
        raise DataError('invalid snapshot identity')
    try:
        row = store.conn.execute('SELECT member_count FROM snapshots WHERE snapshot_id=?', (snapshot_id,)).fetchone()
    except sqlite3.Error as exc:
        raise StorageError('cannot read snapshot directory') from exc
    if row is None:
        raise DataError('unknown snapshot')
    return SnapshotRecord(snapshot_id, validate_members(store, snapshot_id, row[0]))


def create_snapshot(store, new_batch_numbers):
    latest = store.conn.execute('SELECT MAX(snapshot_id) FROM snapshots').fetchone()[0]
    previous = load_snapshot(store, latest)
    numbers = tuple(record.batch_no for record in previous.members) + tuple(new_batch_numbers)
    if len(set(numbers)) != len(numbers):
        raise StorageError('duplicate batch during publication')
    snapshot_id = latest + 1
    store.conn.execute('INSERT INTO snapshots VALUES (?,?)', (snapshot_id, len(numbers)))
    store.conn.executemany('INSERT INTO members VALUES (?,?,?)',
                           ((snapshot_id, position, number) for position, number in enumerate(numbers)))
    return snapshot_id
