"""Hand-calculated pairing cases; no production pairing solver."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

from columnlake.lake.publication import Lake
from columnlake.delivery.decoding import decode_export
from columnlake.source.events import DataError
from columnlake.lake.storage import StorageError


def scalar(values):
    return {'length': len(values), 'validity': [v is not None for v in values],
            'values': [0 if v is None else v for v in values]}


def refs(n):
    return {'length': n, 'validity': [True] * n, 'indices': list(range(n))}


def obs(value, category='g', note=None):
    return {'category': category, 'uv': value, 'note': note}


def rows(values):
    return [{'sample': 1, 'observations': [obs(v) for v in values], 'ignored': 'source'}]


def events(source_rows, wrapped=False):
    flat = [item for row in source_rows for item in (row['observations'] or [])]
    categories = [None if item is None else item['category'] for item in flat]
    readings = [None if item is None else item['uv'] for item in flat]
    category_type = {'dict': {'id': 7, 'value': 'text'}} if wrapped else 'text'
    uv_type = {'dict': {'id': 10, 'value': 'int'}} if wrapped else 'int'
    item_type = {'struct': [['category', category_type], ['uv', uv_type], ['note', 'text']]}
    list_type = {'list': item_type}
    sample_type = {'dict': {'id': 8, 'value': 'int'}} if wrapped else 'int'
    observations_type = {'dict': {'id': 9, 'value': list_type}} if wrapped else list_type
    offsets = [0]
    for row in source_rows:
        offsets.append(offsets[-1] + len(row['observations'] or []))
    lists = {'length': len(source_rows),
             'validity': [row['observations'] is not None for row in source_rows],
             'offsets': offsets,
             'items': {'length': len(flat), 'validity': [v is not None for v in flat],
                       'fields': {
                           'category': refs(len(flat)) if wrapped else scalar(categories),
                           'uv': refs(len(flat)) if wrapped else scalar(readings),
                           'note': scalar([None if v is None else v['note'] for v in flat])}}}
    samples = scalar([row['sample'] for row in source_rows])
    result = [{'kind': 'schema', 'schema': [
        ['sample', sample_type], ['observations', observations_type], ['ignored', 'text']]}]
    if wrapped:
        for number, value_type, values in [
                (7, 'text', scalar(categories)), (10, 'int', scalar(readings)),
                (8, 'int', samples), (9, list_type, lists)]:
            result.append({'kind': 'dictionary', 'id': number, 'type': value_type, 'values': values})
    result.append({'kind': 'batch', 'batch_id': 'b', 'row_count': len(source_rows), 'columns': {
        'sample': refs(len(source_rows)) if wrapped else samples,
        'observations': refs(len(source_rows)) if wrapped else lists,
        'ignored': scalar([row['ignored'] for row in source_rows])}})
    result.append({'kind': 'end'})
    return [dict(event, seq=i) for i, event in enumerate(result)]


def pair(li, ri, luv, ruv, sample=1, category='g'):
    return {'left': [1, 0, li], 'right': [2, 0, ri], 'sample': sample,
            'category': category, 'left_uv': luv, 'right_uv': ruv, 'delta_uv': ruv-luv}


def unmatched(side, address, sample, observation):
    return {'side': side, 'address': address, 'sample': sample, 'observation': observation}


def cases():
    null_rows = [
        {'sample': 1, 'observations': [None, obs(1, None), obs(None), obs(4, note='kept')], 'ignored': 'x'},
        {'sample': None, 'observations': [obs(4)], 'ignored': 'x'},
        {'sample': 1, 'observations': None, 'ignored': 'x'},
        {'sample': 1, 'observations': [], 'ignored': 'x'},
        {'sample': 2, 'observations': [obs(4)], 'ignored': 'x'},
        {'sample': 1, 'observations': [obs(4, 'h')], 'ignored': 'x'},
    ]
    return [
        ('cardinality', rows([4, 0]), rows([3, 5]), 3,
         {'pairs': [pair(0, 1, 4, 5), pair(1, 0, 0, 3)], 'unmatched': []}),
        ('distance', rows([0, 4]), rows([1, 3, 8]), 10,
         {'pairs': [pair(0, 0, 0, 1), pair(1, 1, 4, 3)],
          'unmatched': [unmatched('right', [2, 0, 2], 1, obs(8))]}),
        ('address_tie', rows([0, 1]), rows([4, 3]), 4,
         {'pairs': [pair(0, 0, 0, 4), pair(1, 1, 1, 3)], 'unmatched': []}),
        ('duplicates', rows([5, 5]), rows([5, 5]), 0,
         {'pairs': [pair(0, 0, 5, 5), pair(1, 1, 5, 5)], 'unmatched': []}),
        ('nulls_and_groups', null_rows, rows([4]), 0,
         {'pairs': [pair(3, 0, 4, 4)], 'unmatched': [
             unmatched('left', [1, 0, 0], 1, None),
             unmatched('left', [1, 0, 1], 1, obs(1, None)),
             unmatched('left', [1, 0, 2], 1, obs(None)),
             unmatched('left', [1, 1, 0], None, obs(4)),
             unmatched('left', [1, 4, 0], 2, obs(4)),
             unmatched('left', [1, 5, 0], 1, obs(4, 'h'))]}),
        ('exact_integer', rows([10**80]), rows([10**80 + 3]), 3,
         {'pairs': [pair(0, 0, 10**80, 10**80 + 3)], 'unmatched': []}),
    ]


def fixtures():
    for name, left, right, _, _ in cases():
        for wrapped in (False, True):
            with Lake.open(':memory:') as lake:
                lake.ingest('a', 'A', events(left, wrapped))
                receipt = lake.ingest('b', 'B', events(right, wrapped))
                assert lake.read(receipt.snapshot_id) == left + right, name
                assert decode_export(lake.export(receipt.snapshot_id)) == left + right, name
    print('PAIR_FIXTURES_OK: hand inputs round-trip through unmodified V1')


def rejected(operation, error=DataError):
    try:
        operation()
    except error:
        return
    raise AssertionError('expected ' + error.__name__)


def check_case(path, case, wrapped=False, demonstrate=False):
    name, left, right, tolerance, expected = case
    with Lake.open(path) as lake:
        lake.ingest('a', 'A', events(left, wrapped))
        receipt = lake.ingest('b', 'B', events(right, wrapped))
        snapshot = receipt.snapshot_id
        bundle = lake.export_pairs(snapshot, 'A', 'B', tolerance)
        assert isinstance(bundle, dict)
        document = json.loads(json.dumps(bundle, ensure_ascii=False))
        assert document['version'] == 2
        assert document['snapshot_id'] == snapshot
        assert document['left_stream'] == 'A' and document['right_stream'] == 'B'
        assert document['tolerance'] == tolerance
        assert [m['stream_id'] for m in document['sources']['batches']] == ['A', 'B']
        assert decode_export(lake.export(snapshot)) == left + right
        for args in [(snapshot, 'A', 'A', tolerance), (snapshot, 'A', 'B', True),
                     (snapshot, 'A', 'B', -1), (999999, 'A', 'B', tolerance),
                     (snapshot, '', 'B', tolerance)]:
            rejected(lambda args=args: lake.export_pairs(*args))
        lake.ingest('later', 'C', events(rows([999])))
        assert decode_export(lake.export_pairs(snapshot, 'A', 'B', tolerance)) == expected
        rejected(lambda: decode_export(dict(document, tolerance=True)))
        if document['matches']:
            broken = deepcopy(document)
            broken['matches'].append(deepcopy(broken['matches'][0]))
            rejected(lambda: decode_export(broken))
            broken = deepcopy(document)
            broken['matches'][0][0][1] = 999
            rejected(lambda: decode_export(broken))
        if document['unmatched']:
            broken = deepcopy(document)
            broken['unmatched'].pop()
            rejected(lambda: decode_export(broken))
        if name == 'cardinality':
            rejected(lambda: decode_export(dict(document, tolerance=0)))
        if name == 'nulls_and_groups':
            broken = deepcopy(document)
            broken['matches'][0][0] = [1, 5, 0]
            broken['unmatched'][-1] = ['left', [1, 0, 3]]
            rejected(lambda: decode_export(broken))
        broken = deepcopy(document)
        broken['sources']['batches'][0]['audit']['digest'] = '0' * 64
        rejected(lambda: decode_export(broken))
    # No connection or source query is available to this call.
    actual = decode_export(json.dumps(document, ensure_ascii=False))
    assert actual == expected, (name, expected, actual)
    with Lake.open(path) as reopened:
        assert decode_export(reopened.export_pairs(snapshot, 'A', 'B', tolerance)) == expected
        segment_id = reopened._store.conn.execute('SELECT segment_id FROM batches WHERE batch_no=2').fetchone()[0]
        reopened._store.conn.execute('UPDATE segments SET payload=? WHERE segment_id=?', ('{}', segment_id))
        rejected(lambda: reopened.export_pairs(snapshot, 'A', 'B', tolerance), StorageError)
    if demonstrate:
        print('POINTWISE_SCENARIO ' + json.dumps({
            'case_id': name, 'left_input': left, 'right_input': right,
            'tolerance': tolerance, 'expected': expected, 'actual': actual,
            'offline_bundle': document}, ensure_ascii=False, sort_keys=True))


def verify():
    root = Path(__file__).resolve().parent
    subprocess.run([sys.executable, '-B', '-m', 'unittest', 'discover',
                    '-s', 'tests', '-p', 'test_*.py'], cwd=root, check=True)
    fixtures()
    with TemporaryDirectory(prefix='columnlake-pairs-') as directory:
        for i, case in enumerate(cases()):
            for wrapped in (False, True):
                check_case(Path(directory) / (str(i) + str(wrapped) + '.sqlite'), case, wrapped)
        with Lake.open(Path(directory) / 'multibatch.sqlite') as lake:
            two_batches = events(rows([5]), True)
            extra = deepcopy(two_batches[-2])
            extra['batch_id'] = 'second'
            two_batches.insert(-1, extra)
            two_batches = [dict(event, seq=i) for i, event in enumerate(two_batches)]
            lake.ingest('multi-a', 'A', two_batches)
            receipt = lake.ingest('multi-b', 'B', events(rows([5, 5]), True))
            document = lake.export_pairs(receipt.snapshot_id, 'A', 'B', 0)
            expected = {'pairs': [
                dict(pair(0, 0, 5, 5), right=[3, 0, 0]),
                dict(pair(0, 1, 5, 5), left=[2, 0, 0], right=[3, 0, 1])],
                'unmatched': []}
            assert decode_export(json.dumps(document)) == expected
        with Lake.open(Path(directory) / 'empty.sqlite') as lake:
            assert decode_export(lake.export_pairs(0, 'A', 'B', 0)) == {'pairs': [], 'unmatched': []}
            lake.ingest('incompatible', 'A', [
                {'kind': 'schema', 'seq': 0, 'schema': [['sample', 'text']]},
                {'kind': 'batch', 'seq': 1, 'batch_id': 'bad', 'row_count': 0,
                 'columns': {'sample': scalar([])}}])
            rejected(lambda: lake.export_pairs(1, 'A', 'B', 0))
    print('PAIR_PUBLIC_CASES_OK')


def demo():
    with TemporaryDirectory(prefix='columnlake-pair-demo-') as directory:
        check_case(Path(directory) / 'pair.sqlite', cases()[0], True, True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    options = parser.add_mutually_exclusive_group(required=True)
    options.add_argument('--fixtures', action='store_true')
    options.add_argument('--verify', action='store_true')
    options.add_argument('--demo', action='store_true')
    args = parser.parse_args()
    if args.fixtures:
        fixtures()
    elif args.verify:
        verify()
    else:
        demo()
