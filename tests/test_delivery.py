from copy import deepcopy
import json
import sqlite3
from columnlake.source.events import DataError, SchemaEvent, DictionaryEvent, BatchEvent, EndEvent
from columnlake.lake.publication import Lake
from columnlake.lake.storage import StorageError
from columnlake.delivery.decoding import decode_export, validate_export
from test_intake import LakeCase, D7, SIMPLE_SCHEMA, arr, darr, simple_events


class TestDelivery(LakeCase):
    def test_replacement_delta_and_nested_reference(self):
        outer_type = {'list': {'struct': [['category', D7]]}}
        d9 = {'dict': {'id': 9, 'value': outer_type}}
        schema = [['old_outer', d9], ['current', D7]]
        outer_array = {'length': 3, 'validity': [True, True, False],
                       'offsets': [0, 3, 3, 3],
                       'items': {'length': 3, 'validity': [True, True, True],
                                 'fields': {'category': darr([0, 0, 0], [True, True, False])}}}
        events = [SchemaEvent(0, schema), DictionaryEvent(1, 7, 'text', arr(['甲'])),
                  DictionaryEvent(2, 9, outer_type, outer_array),
                  BatchEvent(3, 'before', {'old_outer': darr([0, 1, 2]), 'current': darr([0, 0, 0])}, 3),
                  DictionaryEvent(4, 7, 'text', arr(['乙'])),
                  DictionaryEvent(5, 7, 'text', arr(['乙', '丙']), True),
                  BatchEvent(6, 'after', {'old_outer': darr([0, 1, 2]), 'current': darr([0, 1, 2])}, 3),
                  EndEvent(7)]
        receipt = self.lake.ingest('nested', 'A', events)
        old = [{'category': '甲'}, {'category': '甲'}, {'category': None}]
        expected = [
            {'old_outer': old, 'current': '甲'}, {'old_outer': [], 'current': '甲'},
            {'old_outer': None, 'current': '甲'}, {'old_outer': old, 'current': '乙'},
            {'old_outer': [], 'current': '乙'}, {'old_outer': None, 'current': '丙'}]
        self.assertEqual(self.lake.read(receipt.snapshot_id), expected)
        self.assertEqual(decode_export(self.lake.export(receipt.snapshot_id)), expected)
        state = self.lake._catalog.load_stream_state('A')
        history = state.dictionaries.history
        self.assertEqual([g.values for k, g in sorted(history.items()) if k.dictionary_id == 7],
                         [('甲',), ('乙',), ('乙', '乙', '丙')])
        self.reopen()
        self.assertEqual(self.lake.read(receipt.snapshot_id), expected)
        self.assertEqual(decode_export(self.lake.export(receipt.snapshot_id)), expected)

    def test_replacement_history(self):
        first = self.lake.ingest('first', 'A', [SchemaEvent(0, SIMPLE_SCHEMA),
            DictionaryEvent(1, 7, 'text', arr(['甲', '乙'])),
            BatchEvent(2, 'old', {'sample': arr([1, 2, 3]), 'category': darr([0, 1, 0])}, 3)])
        old = [{'sample': 1, 'category': '甲'}, {'sample': 2, 'category': '乙'}, {'sample': 3, 'category': '甲'}]
        second = self.lake.ingest('second', 'A', [DictionaryEvent(3, 7, 'text', arr(['丙', '丁'])),
            BatchEvent(4, 'new', {'sample': arr([4]), 'category': darr([1])}, 1)])
        self.assertEqual(self.lake.read(first.snapshot_id), old)
        self.assertEqual(self.lake.read(second.snapshot_id), old + [{'sample': 4, 'category': '丁'}])
        self.assertEqual(decode_export(self.lake.export(first.snapshot_id)), old)

    def test_delta_positions(self):
        events = [SchemaEvent(0, SIMPLE_SCHEMA), DictionaryEvent(1, 7, 'text', arr(['乙', '甲', '乙'])),
                  BatchEvent(2, 'before', {'sample': arr([1]), 'category': darr([2])}, 1),
                  DictionaryEvent(3, 7, 'text', arr(['甲', '丙']), True),
                  BatchEvent(4, 'after', {'sample': arr([2, 3, 4, 5, 6]), 'category': darr([0, 1, 2, 3, 4])}, 5)]
        receipt = self.lake.ingest('delta', 'A', events)
        expected_categories = ['乙', '乙', '甲', '乙', '甲', '丙']
        self.assertEqual(self.lake.read(receipt.snapshot_id, 'category'), expected_categories)
        self.assertEqual([r['category'] for r in decode_export(self.lake.export(receipt.snapshot_id))], expected_categories)
        state = self.lake._catalog.load_stream_state('A')
        self.assertEqual(state.dictionaries.lookup(7).values, ('乙', '甲', '乙', '甲', '丙'))

    def test_mixed_transitive_generations(self):
        d9 = {'dict': {'id': 9, 'value': D7}}
        events = [SchemaEvent(0, [['x', d9]]), DictionaryEvent(1, 7, 'text', arr(['甲'])),
                  DictionaryEvent(2, 9, D7, darr([0])),
                  DictionaryEvent(3, 7, 'text', arr(['乙'])), DictionaryEvent(4, 9, D7, darr([0]), True),
                  BatchEvent(5, 'b', {'x': darr([0, 1, 0])}, 3)]
        receipt = self.lake.ingest('mixed', 'A', events)
        expected = [{'x': '甲'}, {'x': '乙'}, {'x': '甲'}]
        self.assertEqual(self.lake.read(receipt.snapshot_id), expected)
        self.assertEqual(decode_export(self.lake.export(receipt.snapshot_id)), expected)
        self.reopen()
        self.assertEqual(self.lake.read(receipt.snapshot_id), expected)

    def test_cross_stream_independent_export(self):
        first = self.lake.ingest('A', 'A', simple_events('甲'))
        second = self.lake.ingest('B', 'B', simple_events('乙'))
        bundle = json.loads(self.lake.export(second.snapshot_id).payload)
        self.assertEqual([b['stream_id'] for b in bundle['batches']], ['A', 'B'])
        self.assertEqual([b['encoded']['dictionaries'][0]['id'] for b in bundle['batches']], [0, 0])
        expected = [{'sample': 1, 'category': '甲'}, {'sample': 1, 'category': '乙'}]
        self.lake.close()
        try:
            self.assertEqual(decode_export(json.dumps(bundle, ensure_ascii=False)), expected)
            validate_export(bundle)
        finally:
            self.lake = Lake.open(self.path)
        self.assertEqual(self.lake.read(first.snapshot_id), expected[:1])
        self.assertEqual(self.lake.read(second.snapshot_id), expected)
        broken = deepcopy(bundle)
        broken['batches'][0]['encoded']['columns']['category']['references'][0] = 999
        with self.assertRaises(DataError):
            decode_export(broken)
        reordered = deepcopy(bundle)
        reordered['batches'].reverse()
        with self.assertRaises(DataError):
            decode_export(reordered)

    def test_reopen_and_replay(self):
        events = simple_events()
        receipt = self.lake.ingest('request', 'A', events)
        counts = self.table_counts()
        self.reopen()
        self.assertEqual(self.lake.ingest('request', 'A', deepcopy(events)), receipt)
        self.assertEqual(self.table_counts(), counts)
        self.assertEqual(self.lake.read(receipt.snapshot_id), [{'sample': 1, 'category': '甲'}])
        for changed_stream, changed in [('B', events), ('A', simple_events('乙')),
                                        ('A', list(reversed(events)))]:
            with self.assertRaises(DataError):
                self.lake.ingest('request', changed_stream, changed)
            self.assertEqual(self.table_counts(), counts)
        next_receipt = self.lake.ingest('user-addition', 'B', simple_events('用户类别'))
        self.reopen()
        self.assertEqual(self.lake.read(next_receipt.snapshot_id)[-1]['category'], '用户类别')
        self.assertEqual(self.lake.read(receipt.snapshot_id), [{'sample': 1, 'category': '甲'}])

    def test_format_rejection(self):
        self.lake.ingest('initial', 'A', simple_events())
        self.lake.close()
        connection = sqlite3.connect(self.path)
        connection.execute('UPDATE meta SET value=? WHERE key=?', ('99', 'format'))
        connection.commit()
        connection.close()
        try:
            with self.assertRaises(StorageError):
                Lake.open(self.path)
            connection = sqlite3.connect(self.path)
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 1)
            self.assertEqual(connection.execute('SELECT value FROM meta WHERE key=?', ('format',)).fetchone()[0], '99')
            connection.execute('UPDATE meta SET value=? WHERE key=?', ('1', 'format'))
            connection.commit()
            connection.close()
        finally:
            self.lake = Lake.open(self.path)
        self.assertEqual(self.lake.read(1), [{'sample': 1, 'category': '甲'}])
