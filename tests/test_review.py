import unittest
from columnlake.source.events import DataError, SchemaEvent, DictionaryEvent, BatchEvent
from columnlake.delivery.decoding import decode_export
from test_intake import LakeCase, D7, arr, darr, simple_events


def review_events():
    item_type = {'struct': [['category', D7], ['uv', 'int']]}
    schema = [['observations', {'list': item_type}], ['parent', {'struct': [['category', D7]]}]]
    child = {'length': 4, 'validity': [True, True, False, True],
             'fields': {'category': darr([0, 0, 1, 1]), 'uv': arr([10, 11, 999, 999])}}
    observations = {'length': 4, 'validity': [True, True, False, True],
                    'offsets': [0, 3, 3, 4, 4], 'items': child}
    parent = {'length': 4, 'validity': [False, True, True, True],
              'fields': {'category': darr([1, 0, 0, 0], [True, True, False, True])}}
    return [SchemaEvent(0, schema), DictionaryEvent(1, 7, 'text', arr(['甲', '占位'])),
            BatchEvent(2, 'review', {'observations': observations, 'parent': parent}, 4)]


def review_expected():
    return [
        {'observations': [{'category': '甲', 'uv': 10}, {'category': '甲', 'uv': 11}, None], 'parent': None},
        {'observations': [], 'parent': {'category': '甲'}},
        {'observations': None, 'parent': {'category': None}},
        {'observations': [], 'parent': {'category': '甲'}},
    ]


class TestReview(LakeCase):
    def test_null_empty_and_duplicates(self):
        receipt = self.lake.ingest('review', 'A', review_events())
        expected = review_expected()
        self.assertEqual(self.lake.read(receipt.snapshot_id), expected)
        self.assertEqual(decode_export(self.lake.export(receipt.snapshot_id)), expected)
        self.assertEqual(self.lake.read(receipt.snapshot_id, 'observations.category'),
                         [['甲', '甲', None], [], None, []])
        self.assertEqual(self.lake.profile(receipt.snapshot_id, 'observations.category').to_obj(),
                         {'counts': [{'type': 'text', 'value': '甲', 'count': 2}], 'nulls': 2})
        self.assertEqual(self.lake.profile(receipt.snapshot_id, 'parent.category').to_obj(),
                         {'counts': [{'type': 'text', 'value': '甲', 'count': 2}], 'nulls': 2})
        audit = self.lake.export(receipt.snapshot_id).to_obj()['batches'][0]['audit']
        self.assertEqual((audit['row_count'], audit['leaf_values'], audit['nulls']), (4, 6, 4))

    def test_null_parent_projection(self):
        receipt = self.lake.ingest('review', 'A', review_events())
        self.assertEqual(self.lake.read(receipt.snapshot_id, ['parent', 'category']),
                         [None, '甲', None, '甲'])
        self.assertEqual(self.lake.read(receipt.snapshot_id, 'observations.uv'),
                         [[10, 11, None], [], None, []])
        self.assertNotIn('占位', repr(self.lake.read(receipt.snapshot_id)))

    def test_occurrence_counts(self):
        nested = {'list': {'list': 'int'}}
        leaf = arr([5, 5, 0], [True, True, False])
        inner = {'length': 3, 'validity': [True, True, False],
                 'offsets': [0, 3, 3, 3], 'items': leaf}
        outer = {'length': 2, 'validity': [True, True],
                 'offsets': [0, 2, 3], 'items': inner}
        receipt = self.lake.ingest('nested', 'N', [SchemaEvent(0, [['x', nested]]),
                                  BatchEvent(1, 'b', {'x': outer}, 2)])
        self.assertEqual(self.lake.read(receipt.snapshot_id, 'x'), [[[5, 5, None], []], [None]])
        self.assertEqual(self.lake.profile(receipt.snapshot_id, 'x').to_obj(),
                         {'counts': [{'type': 'int', 'value': 5, 'count': 2}], 'nulls': 2})

    def test_fixed_snapshot(self):
        receipt = self.lake.ingest('review', 'A', review_events())
        original = self.lake.read(receipt.snapshot_id)
        original_profile = self.lake.profile(receipt.snapshot_id, 'parent.category')
        self.lake.ingest('other', 'B', simple_events('乙'))
        self.assertEqual(self.lake.read(receipt.snapshot_id), original)
        self.assertEqual(self.lake.profile(receipt.snapshot_id, 'parent.category'), original_profile)
        returned = self.lake.read(receipt.snapshot_id)
        returned[0]['observations'][0]['category'] = '用户修改的返回对象'
        self.assertEqual(self.lake.read(receipt.snapshot_id), original)

    def test_invalid_path(self):
        receipt = self.lake.ingest('review', 'A', review_events())
        for path in ('missing', 'parent.missing', 'observations.uv.bad', '', ['parent', '']):
            with self.subTest(path=path), self.assertRaises(DataError):
                self.lake.read(receipt.snapshot_id, path)
            with self.assertRaises(DataError):
                self.lake.profile(receipt.snapshot_id, path)
        for path in ('parent', 'observations'):
            with self.assertRaises(DataError):
                self.lake.profile(receipt.snapshot_id, path)
        for operation in (self.lake.read, self.lake.export):
            with self.assertRaises(DataError):
                operation(999)
        with self.assertRaises(DataError):
            self.lake.profile(999, 'parent.category')
        self.assertEqual(self.lake.read(0), [])
        self.assertEqual(decode_export(self.lake.export(0)), [])
        with self.assertRaises(DataError):
            self.lake.read(0, 'parent')


if __name__ == '__main__':
    unittest.main()
