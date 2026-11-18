import hashlib
import json
from columnlake.source.events import DataError, EndEvent
from columnlake.lake.storage import StorageError
from columnlake.delivery.decoding import decode_export
from test_intake import LakeCase, simple_events


class SourceFailure(RuntimeError):
    pass


class TestFailures(LakeCase):
    def test_source_exception(self):
        old = self.lake.ingest('old', 'A', simple_events())
        before = self.table_counts()
        old_export = self.lake.export(old.snapshot_id).payload
        failure = SourceFailure('instrument stopped after a complete batch')

        def source():
            yield from simple_events('乙', end=False)
            raise failure

        with self.assertRaises(SourceFailure) as caught:
            self.lake.ingest('new', 'B', source())
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.table_counts(), before)
        self.assertEqual(self.lake.export(old.snapshot_id).payload, old_export)
        receipt = self.lake.ingest('new', 'B', simple_events('乙'))
        self.assertEqual(self.lake.read(receipt.snapshot_id), [
            {'sample': 1, 'category': '甲'}, {'sample': 1, 'category': '乙'}])

    def test_replay_consumes_source(self):
        events = simple_events()
        receipt = self.lake.ingest('request', 'A', events)
        before = self.table_counts()
        failure = SourceFailure('failure after end must remain visible')

        def source():
            yield from events
            raise failure

        with self.assertRaises(SourceFailure) as caught:
            self.lake.ingest('request', 'A', source())
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.table_counts(), before)
        self.assertEqual(self.lake.ingest('request', 'A', events), receipt)

    def test_publication_rollback(self):
        old = self.lake.ingest('old', 'A', simple_events())
        expected = [{'sample': 1, 'category': '甲'}]
        for index, stage in enumerate(('after_segments', 'after_catalog', 'after_snapshot', 'after_receipt')):
            with self.subTest(stage=stage):
                before = self.table_counts()
                failure = RuntimeError('controlled publication interruption: ' + stage)
                visited = []

                def fault(actual_stage):
                    visited.append(actual_stage)
                    if actual_stage == stage:
                        raise failure

                self.lake.fault = fault
                request_id = 'new-' + str(index)
                stream_id = 'B-' + str(index)
                with self.assertRaises(RuntimeError) as caught:
                    self.lake.ingest(request_id, stream_id, simple_events('乙'))
                self.assertIs(caught.exception, failure)
                self.assertIn(stage, visited)
                self.lake.fault = None
                self.assertEqual(self.table_counts(), before)
                self.reopen()
                self.assertEqual(self.table_counts(), before)
                self.assertEqual(self.lake.read(old.snapshot_id), expected[:1])
                self.assertIsNone(self.lake._catalog.find_receipt(request_id))
                self.assertIsNone(self.lake._catalog.load_stream_state(stream_id).schema)
                receipt = self.lake.ingest(request_id, stream_id, simple_events('乙'))
                expected.append({'sample': 1, 'category': '乙'})
                self.assertEqual(self.lake.read(receipt.snapshot_id), expected)
                counts = self.table_counts()
                self.assertEqual(self.lake.ingest(request_id, stream_id, simple_events('乙')), receipt)
                self.assertEqual(self.table_counts(), counts)

    def test_damaged_segment(self):
        old = self.lake.ingest('old', 'A', simple_events())
        new = self.lake.ingest('new', 'B', simple_events('乙'))
        connection = self.lake._store.conn
        segment_id = connection.execute('SELECT MAX(segment_id) FROM segments').fetchone()[0]
        connection.execute('UPDATE segments SET payload=? WHERE segment_id=?', ('{}', segment_id))
        for operation in (self.lake.read, self.lake.export):
            with self.assertRaises(StorageError):
                operation(new.snapshot_id)
        self.assertEqual(self.lake.read(old.snapshot_id), [{'sample': 1, 'category': '甲'}])
        self.assertEqual(decode_export(self.lake.export(old.snapshot_id)), [{'sample': 1, 'category': '甲'}])
        self.reopen()
        with self.assertRaises(StorageError):
            self.lake.read(new.snapshot_id)
        with self.assertRaises(StorageError):
            self.lake.export(new.snapshot_id)

    def test_valid_checksum_does_not_hide_bad_encoding_or_audit(self):
        receipt = self.lake.ingest('old', 'A', simple_events())
        connection = self.lake._store.conn
        original = connection.execute('SELECT payload FROM segments WHERE segment_id=1').fetchone()[0]
        for mutation in ('bad-index', 'wrong-value', 'missing-dictionary'):
            raw = json.loads(original)
            if mutation == 'bad-index':
                raw['columns']['category']['indices'][0] = 99
            elif mutation == 'wrong-value':
                raw['dictionaries'][0]['array']['values'][0] = '被篡改'
            else:
                raw['dictionaries'] = []
            payload = json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
            data = payload.encode('utf-8')
            connection.execute('UPDATE segments SET payload=?,checksum=?,byte_length=? WHERE segment_id=1',
                               (payload, hashlib.sha256(data).hexdigest(), len(data)))
            with self.subTest(mutation=mutation):
                with self.assertRaises(StorageError):
                    self.lake.read(receipt.snapshot_id)
                with self.assertRaises(StorageError):
                    self.lake.export(receipt.snapshot_id)

    def test_failed_active_continuation_preserves_cursor(self):
        first = self.lake.ingest('first', 'A', simple_events(end=False))
        before = self.table_counts()
        with self.assertRaises(DataError):
            self.lake.ingest('bad', 'A', [EndEvent(4)])
        self.assertEqual(self.table_counts(), before)
        ended = self.lake.ingest('end', 'A', [EndEvent(3)])
        self.assertEqual(self.lake.read(ended.snapshot_id), self.lake.read(first.snapshot_id))
        self.assertEqual(self.lake.ingest('end', 'A', [EndEvent(3)]), ended)
