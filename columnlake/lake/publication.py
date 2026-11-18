import argparse
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from columnlake.source.events import DataError, normalize_event
from columnlake.source.identity import identify_request, same_request
from columnlake.source.session import IngestSession
from columnlake.lake.storage import Store
from columnlake.lake.catalog import LakeCatalog, IntakeReceipt
from columnlake.lake.segments import write_segment
from columnlake.lake.snapshots import create_snapshot
from columnlake.delivery.reader import SnapshotReader
from columnlake.delivery.counts import profile
from columnlake.delivery.export import SnapshotExporter


class Lake:
    def __init__(self, store, fault=None):
        self._store = store
        self._catalog = LakeCatalog(store)
        self.fault = fault

    @classmethod
    def open(cls, path, fault=None):
        return cls(Store.open(path), fault)

    def close(self):
        self._store.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _checkpoint(self, stage):
        if self.fault is not None:
            self.fault(stage)

    def ingest(self, request_id, stream_id, events):
        if type(request_id) is not str or not request_id:
            raise DataError('request_id must be nonempty text')
        if type(stream_id) is not str or not stream_id:
            raise DataError('stream_id must be nonempty text')
        normalized = tuple(normalize_event(event) for event in events)
        identity = identify_request(stream_id, normalized)
        previous = self._catalog.find_receipt(request_id)
        if previous is not None:
            old_identity, receipt = previous
            if not same_request(old_identity, identity):
                raise DataError('request_id is already bound to different content')
            return receipt
        state = self._catalog.load_stream_state(stream_id)
        prepared = IngestSession(state).prepare(normalized)
        with self._store.transaction():
            segment_ids = tuple(write_segment(self._store, batch) for batch in prepared.batches)
            self._checkpoint('after_segments')
            self._catalog.save_stream_state(prepared.state)
            numbers = self._catalog.record_batches(prepared.batches, segment_ids)
            self._checkpoint('after_catalog')
            snapshot_id = create_snapshot(self._store, numbers)
            self._checkpoint('after_snapshot')
            receipt = IntakeReceipt(request_id, stream_id, snapshot_id, numbers)
            self._catalog.write_receipt(identity, receipt)
            self._checkpoint('after_receipt')
        return receipt

    def read(self, snapshot_id, projection=None):
        return SnapshotReader(self._store).read(snapshot_id, projection)

    def profile(self, snapshot_id, categorical_path):
        return profile(self._store, snapshot_id, categorical_path)

    def export(self, snapshot_id):
        return SnapshotExporter(self._store).export(snapshot_id)

    def export_pairs(self, snapshot_id, left_stream, right_stream, tolerance):
        return SnapshotExporter(self._store).export_pairs(
            snapshot_id, left_stream, right_stream, tolerance)


def demo():
    from columnlake.delivery.decoding import decode_export
    root = Path(__file__).resolve().parents[2]
    requests = json.loads((root / 'sample-events.json').read_text(encoding='utf-8'))['requests']
    expected = json.loads((root / 'sample-expected.json').read_text(encoding='utf-8'))
    with TemporaryDirectory(prefix='columnlake-demo-') as directory:
        path = Path(directory) / 'research.sqlite'
        with Lake.open(path) as lake:
            receipts = [lake.ingest(r['request_id'], r['stream_id'], r['events']) for r in requests]
            snapshot = receipts[-1].snapshot_id
            actual = lake.read(snapshot)
            projected = lake.read(snapshot, 'observations.category')
            measured = lake.profile(snapshot, 'observations.category').to_obj()
            exported = lake.export(snapshot).to_obj()
            downstream = decode_export(exported)
            if actual != expected['rows'] or projected != expected['projection']:
                raise AssertionError('sample logical values differ from hand calculation')
            if measured != expected['profile'] or downstream != expected['rows']:
                raise AssertionError('sample quality/training delivery mismatch')
        with Lake.open(path) as reopened:
            if reopened.read(snapshot) != expected['rows']:
                raise AssertionError('reopen changed sample values')
            replay = reopened.ingest(requests[0]['request_id'], requests[0]['stream_id'], requests[0]['events'])
            if replay != receipts[0] or reopened.read(snapshot) != expected['rows']:
                raise AssertionError('replay changed committed sample data')
        print('POINTWISE_SCENARIO ' + json.dumps({
            'case_id': 'v1', 'input': requests,
            'expected': {'rows': expected['rows'], 'profile': expected['profile']},
            'actual': {'rows': actual, 'profile': measured},
            'downstream_expected': expected['rows'], 'downstream_actual': downstream,
            'code_paths': ['columnlake/source/session.py', 'columnlake/dictionary/references.py',
                           'columnlake/assembly/batches.py', 'columnlake/lake/publication.py',
                           'columnlake/delivery/counts.py', 'columnlake/delivery/decoding.py'],
            'oracle': 'sample-expected.json contains separately hand-calculated ordered rows and occurrence counts'
        }, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ColumnLake research data demonstration')
    parser.add_argument('--demo', action='store_true', required=True)
    parser.parse_args()
    demo()
