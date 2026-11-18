from contextlib import contextmanager
from pathlib import Path
import sqlite3


class StorageError(RuntimeError):
    pass


_TABLES = {
    'meta': 'CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)',
    'streams': 'CREATE TABLE streams(stream_id TEXT PRIMARY KEY, state TEXT NOT NULL)',
    'segments': 'CREATE TABLE segments(segment_id INTEGER PRIMARY KEY, payload TEXT NOT NULL, checksum TEXT NOT NULL, byte_length INTEGER NOT NULL, audit TEXT NOT NULL)',
    'batches': 'CREATE TABLE batches(batch_no INTEGER PRIMARY KEY, stream_id TEXT NOT NULL REFERENCES streams(stream_id), batch_id TEXT NOT NULL, segment_id INTEGER NOT NULL UNIQUE REFERENCES segments(segment_id), audit TEXT NOT NULL, UNIQUE(stream_id,batch_id))',
    'snapshots': 'CREATE TABLE snapshots(snapshot_id INTEGER PRIMARY KEY, member_count INTEGER NOT NULL)',
    'members': 'CREATE TABLE members(snapshot_id INTEGER NOT NULL REFERENCES snapshots(snapshot_id), position INTEGER NOT NULL, batch_no INTEGER NOT NULL REFERENCES batches(batch_no), PRIMARY KEY(snapshot_id,position), UNIQUE(snapshot_id,batch_no))',
    'receipts': 'CREATE TABLE receipts(request_id TEXT PRIMARY KEY, content TEXT NOT NULL, digest TEXT NOT NULL, snapshot_id INTEGER NOT NULL UNIQUE REFERENCES snapshots(snapshot_id), receipt TEXT NOT NULL)',
}


class Store:
    def __init__(self, connection):
        self.conn = connection

    @classmethod
    def open(cls, path):
        path = str(path)
        existed = path != ':memory:' and Path(path).exists()
        connection = None
        try:
            connection = sqlite3.connect(path, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA foreign_keys=ON')
            store = cls(connection)
            if not existed:
                with store.transaction():
                    for statement in _TABLES.values():
                        connection.execute(statement)
                    connection.execute('INSERT INTO meta VALUES (?,?)', ('format', '1'))
                    connection.execute('INSERT INTO snapshots VALUES (0,0)')
            store.verify_format()
            return store
        except (sqlite3.Error, OSError, StorageError) as exc:
            if connection is not None:
                connection.close()
            if isinstance(exc, StorageError):
                raise
            raise StorageError('cannot open lake storage') from exc

    def close(self):
        self.conn.close()

    @contextmanager
    def transaction(self):
        try:
            self.conn.execute('BEGIN IMMEDIATE')
            try:
                yield
                self.conn.execute('COMMIT')
            except BaseException:
                if self.conn.in_transaction:
                    self.conn.execute('ROLLBACK')
                raise
        except sqlite3.Error as exc:
            raise StorageError('publication transaction failed') from exc

    def verify_format(self):
        try:
            names = {r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if names != set(_TABLES):
                raise StorageError('unsupported lake directory')
            version = self.conn.execute('SELECT value FROM meta WHERE key=?', ('format',)).fetchone()
            if version is None or version[0] != '1':
                raise StorageError('unsupported lake format')
            if self.conn.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise StorageError('SQLite integrity check failed')
            if self.conn.execute('PRAGMA foreign_key_check').fetchone() is not None:
                raise StorageError('broken lake reference')
            segment_ids = {r[0] for r in self.conn.execute('SELECT segment_id FROM segments')}
            batch_rows = list(self.conn.execute('SELECT batch_no,segment_id FROM batches ORDER BY batch_no'))
            if segment_ids != {r['segment_id'] for r in batch_rows}:
                raise StorageError('orphan lake segment')
            if [r['batch_no'] for r in batch_rows] != list(range(1, len(batch_rows) + 1)):
                raise StorageError('broken batch sequence')
            snapshots = list(self.conn.execute('SELECT snapshot_id,member_count FROM snapshots ORDER BY snapshot_id'))
            if not snapshots or [r[0] for r in snapshots] != list(range(len(snapshots))):
                raise StorageError('broken snapshot sequence')
            previous = []
            for snapshot in snapshots:
                rows = list(self.conn.execute('SELECT position,batch_no FROM members WHERE snapshot_id=? ORDER BY position', (snapshot[0],)))
                ids = [r['batch_no'] for r in rows]
                if snapshot[1] != len(rows) or [r['position'] for r in rows] != list(range(len(rows))):
                    raise StorageError('broken snapshot membership')
                if ids[:len(previous)] != previous or (snapshot[0] == 0 and ids):
                    raise StorageError('snapshot history is not append-only')
                previous = ids
            if previous != [r['batch_no'] for r in batch_rows]:
                raise StorageError('latest snapshot omits committed batches')
            receipt_snapshots = [r[0] for r in self.conn.execute('SELECT snapshot_id FROM receipts ORDER BY snapshot_id')]
            if receipt_snapshots != list(range(1, len(snapshots))):
                raise StorageError('snapshot/receipt directory mismatch')
        except sqlite3.Error as exc:
            raise StorageError('damaged lake directory') from exc
