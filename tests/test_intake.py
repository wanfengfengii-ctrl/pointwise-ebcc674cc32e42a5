import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from columnlake.source.events import DataError, SchemaEvent, DictionaryEvent, BatchEvent, EndEvent
from columnlake.lake.publication import Lake
from columnlake.delivery.decoding import decode_export


D7 = {'dict': {'id': 7, 'value': 'text'}}
SIMPLE_SCHEMA = [['sample', 'int'], ['category', D7]]
ROOT = Path(__file__).resolve().parents[1]


def arr(values, validity=None):
    return {'length': len(values), 'validity': [True] * len(values) if validity is None else validity,
            'values': values}


def darr(indices, validity=None):
    return {'length': len(indices), 'validity': [True] * len(indices) if validity is None else validity,
            'indices': indices}


def simple_events(value='甲', batch_id='b', end=True):
    events = [
        {'kind': 'schema', 'seq': 0, 'schema': SIMPLE_SCHEMA},
        {'kind': 'dictionary', 'seq': 1, 'id': 7, 'type': 'text', 'values': arr([value])},
        {'kind': 'batch', 'seq': 2, 'batch_id': batch_id, 'row_count': 1,
         'columns': {'sample': arr([1]), 'category': darr([0])}},
    ]
    if end:
        events.append({'kind': 'end', 'seq': 3})
    return events


class LakeCase(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(prefix='columnlake-test-')
        self.path = Path(self.temp.name) / 'lake.sqlite'
        self.lake = Lake.open(self.path)

    def tearDown(self):
        self.lake.close()
        self.temp.cleanup()

    def reopen(self):
        self.lake.close()
        self.lake = Lake.open(self.path)

    def table_counts(self):
        return {name: self.lake._store.conn.execute('SELECT COUNT(*) FROM ' + name).fetchone()[0]
                for name in ('streams', 'segments', 'batches', 'snapshots', 'members', 'receipts')}


class TestDailyIntake(LakeCase):
    def test_two_instruments(self):
        requests = json.loads((ROOT / 'sample-events.json').read_text(encoding='utf-8'))['requests']
        expected = json.loads((ROOT / 'sample-expected.json').read_text(encoding='utf-8'))
        first = self.lake.ingest(**requests[0])
        old_rows = self.lake.read(first.snapshot_id)
        second = self.lake.ingest(**requests[1])
        actual = self.lake.read(second.snapshot_id)
        downstream = decode_export(self.lake.export(second.snapshot_id))
        self.assertEqual(actual, expected['rows'])
        self.assertEqual(downstream, expected['rows'])
        self.assertEqual(old_rows, expected['rows'][:4])
        self.assertEqual(self.lake.read(first.snapshot_id), old_rows)
        self.assertEqual([r['sample'] for r in actual], [101, 102, 103, 104, 201])
        print('POINTWISE_SCENARIO ' + json.dumps({
            'case_id': 'v1', 'input': requests, 'expected': expected['rows'], 'actual': actual,
            'downstream_expected': expected['rows'], 'downstream_actual': downstream,
            'code_paths': ['columnlake/source/session.py', 'columnlake/assembly/batches.py',
                           'columnlake/lake/encoding.py', 'columnlake/delivery/decoding.py'],
            'oracle': 'separate hand-calculated sample rows; literal sample order 101,102,103,104,201'
        }, ensure_ascii=False, sort_keys=True))

    def test_schema_arrays(self):
        huge = 10 ** 100
        events = [SchemaEvent(0, [['reading', 'int'], ['label', 'text']]),
                  BatchEvent(1, 'typed', {'reading': arr([huge, -huge]),
                                          'label': arr(['é', 'é'])}, 2), EndEvent(2)]
        receipt = self.lake.ingest('typed', 'instrument', events)
        expected = [{'reading': huge, 'label': 'é'}, {'reading': -huge, 'label': 'é'}]
        self.assertEqual(self.lake.read(receipt.snapshot_id), expected)
        self.assertEqual(decode_export(self.lake.export(receipt.snapshot_id)), expected)
        for value in (True, 1.5, '3'):
            with self.subTest(value=value), self.assertRaises(DataError):
                self.lake.ingest('bad-' + repr(value), 'bad-' + repr(value), [
                    SchemaEvent(0, [['reading', 'int']]),
                    BatchEvent(1, 'bad', {'reading': arr([value])}, 1)])

    def test_event_order(self):
        first = self.lake.ingest('first', 'A', simple_events(end=False))
        continued = [BatchEvent(3, 'next', {'sample': arr([2]), 'category': darr([0])}, 1), EndEvent(4)]
        final = self.lake.ingest('continue', 'A', continued)
        self.assertEqual(self.lake.read(first.snapshot_id), [{'sample': 1, 'category': '甲'}])
        self.assertEqual(self.lake.read(final.snapshot_id), [
            {'sample': 1, 'category': '甲'}, {'sample': 2, 'category': '甲'}])
        self.assertEqual(self.lake.ingest('continue', 'A', continued), final)
        for events in ([EndEvent(5)], [SchemaEvent(5, SIMPLE_SCHEMA)]):
            with self.assertRaises(DataError):
                self.lake.ingest('ended-' + str(len(events)), 'A', events)
        for events in ([EndEvent(0)], [SchemaEvent(1, SIMPLE_SCHEMA)],
                       [SchemaEvent(0, SIMPLE_SCHEMA), SchemaEvent(1, SIMPLE_SCHEMA)]):
            with self.assertRaises(DataError):
                self.lake.ingest('order-' + repr(events), 'new', events)
        with self.assertRaises(DataError):
            self.lake.ingest('duplicate', 'other', [SchemaEvent(0, SIMPLE_SCHEMA),
                DictionaryEvent(1, 7, 'text', arr(['甲'])),
                BatchEvent(2, 'b', {'sample': arr([1]), 'category': darr([0])}, 1),
                BatchEvent(3, 'b', {'sample': arr([2]), 'category': darr([0])}, 1)])

    def test_batch_order_audit(self):
        receipt = self.lake.ingest('audit', 'A', simple_events())
        exported = self.lake.export(receipt.snapshot_id).to_obj()
        document = {'format': 1, 'schema': SIMPLE_SCHEMA, 'rows': [
            ['struct', [['sample', ['int', '1']], ['category', ['text', '甲']]]]]}
        text = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        expected = {'row_count': 1, 'leaf_values': 2, 'nulls': 0,
                    'digest': hashlib.sha256(text.encode('utf-8')).hexdigest()}
        self.assertEqual(exported['batches'][0]['audit'], expected)
        reordered = [SchemaEvent(0, [['x', 'int']]),
                     BatchEvent(1, 'b', {'x': arr([2, 1])}, 2)]
        other = self.lake.ingest('ordered', 'B', reordered)
        self.assertEqual(self.lake.read(other.snapshot_id)[-2:], [{'x': 2}, {'x': 1}])

    def test_invalid_dictionary_reference(self):
        receipt = self.lake.ingest('good', 'A', simple_events())
        before = self.table_counts()
        old_export = self.lake.export(receipt.snapshot_id).payload
        cases = [
            [SchemaEvent(0, SIMPLE_SCHEMA), BatchEvent(1, 'x', {'sample': arr([1]), 'category': darr([0])}, 1)],
            [SchemaEvent(0, SIMPLE_SCHEMA), DictionaryEvent(1, 7, 'text', arr(['甲'])),
             BatchEvent(2, 'x', {'sample': arr([1]), 'category': darr([1])}, 1)],
            [SchemaEvent(0, SIMPLE_SCHEMA), DictionaryEvent(1, 7, 'int', arr([5])),
             BatchEvent(2, 'x', {'sample': arr([1]), 'category': darr([0])}, 1)],
            [SchemaEvent(0, SIMPLE_SCHEMA), DictionaryEvent(1, 7, 'text', arr(['甲'])),
             BatchEvent(2, 'x', {'sample': arr([1]), 'category': darr([-1])}, 1)],
            [SchemaEvent(0, SIMPLE_SCHEMA), DictionaryEvent(1, 9, D7, darr([0]))],
            [SchemaEvent(0, SIMPLE_SCHEMA), DictionaryEvent(1, 7, D7, darr([0]))],
            [SchemaEvent(0, SIMPLE_SCHEMA), DictionaryEvent(1, 7, 'text', arr(['甲']), True)],
        ]
        for number, events in enumerate(cases):
            with self.subTest(number=number), self.assertRaises(DataError):
                self.lake.ingest('bad-' + str(number), 'bad-' + str(number), events)
            self.assertEqual(self.table_counts(), before)
            self.assertEqual(self.lake.export(receipt.snapshot_id).payload, old_export)
        null_event = [SchemaEvent(0, SIMPLE_SCHEMA), BatchEvent(1, 'null',
                      {'sample': arr([2]), 'category': darr([-999], [False])}, 1)]
        result = self.lake.ingest('null', 'null-stream', null_event)
        self.assertEqual(self.lake.read(result.snapshot_id)[-1], {'sample': 2, 'category': None})

    def test_array_rejection(self):
        receipt = self.lake.ingest('good', 'A', simple_events())
        before = self.table_counts()
        list_type = {'list': D7}
        malformed = [
            {'length': 1, 'validity': [], 'offsets': [0, 1], 'items': darr([0])},
            {'length': 1, 'validity': [True], 'offsets': [1, 1], 'items': darr([0])},
            {'length': 2, 'validity': [True, True], 'offsets': [0, 2, 1], 'items': darr([0])},
            {'length': 1, 'validity': [True], 'offsets': [0, 2], 'items': darr([0])},
            {'length': 1, 'validity': [True], 'offsets': [0, 1],
             'items': {'length': 1, 'validity': [True], 'indices': []}},
        ]
        for number, column in enumerate(malformed):
            with self.subTest(number=number), self.assertRaises(DataError):
                self.lake.ingest('layout-' + str(number), 'layout-' + str(number), [
                    SchemaEvent(0, [['x', list_type]]), DictionaryEvent(1, 7, 'text', arr(['甲'])),
                    BatchEvent(2, 'b', {'x': column}, column['length'])])
            self.assertEqual(self.table_counts(), before)
        with self.assertRaises(DataError):
            self.lake.ingest('struct', 'struct', [SchemaEvent(0, [['x', {'struct': [['a', 'int']]}]]),
                BatchEvent(1, 'b', {'x': {'length': 1, 'validity': [True], 'fields': {'a': arr([])}}}, 1)])
        self.assertEqual(self.lake.read(receipt.snapshot_id), [{'sample': 1, 'category': '甲'}])
