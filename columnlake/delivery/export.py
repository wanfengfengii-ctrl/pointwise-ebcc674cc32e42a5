from dataclasses import dataclass
import json
from columnlake.source.events import DataError, exact_int
from columnlake.source.identity import canonical_json
from columnlake.delivery.reader import SnapshotReader
from columnlake.delivery.pairing import require_pair_schema, collect_occurrences, plan_pairs


@dataclass(frozen=True)
class ExportBundle:
    payload: str

    def to_obj(self):
        return json.loads(self.payload)


class SnapshotExporter:
    def __init__(self, store):
        self.store = store

    def export(self, snapshot_id):
        batches = []
        for record, segment in SnapshotReader(self.store).iter_batches(snapshot_id):
            batches.append({'batch_no': record.batch_no, 'stream_id': record.stream_id,
                            'batch_id': record.batch_id, 'audit': segment.audit.to_obj(),
                            'encoded': segment.encoded.to_obj()})
        return ExportBundle(canonical_json({'version': 1, 'snapshot_id': snapshot_id,
                                            'batches': batches}))

    def export_pairs(self, snapshot_id, left_stream, right_stream, tolerance):
        if not exact_int(snapshot_id):
            raise DataError('invalid snapshot identity')
        if type(left_stream) is not str or not left_stream:
            raise DataError('left_stream must be nonempty text')
        if type(right_stream) is not str or not right_stream:
            raise DataError('right_stream must be nonempty text')
        if left_stream == right_stream:
            raise DataError('pair streams must differ')
        if not exact_int(tolerance):
            raise DataError('tolerance must be a nonnegative integer')
        batches = []
        occurrences = {'left': [], 'right': []}
        for record, segment in SnapshotReader(self.store).iter_batches(snapshot_id):
            if record.stream_id == left_stream:
                side = 'left'
            elif record.stream_id == right_stream:
                side = 'right'
            else:
                continue
            require_pair_schema(segment.schema)
            batches.append({'batch_no': record.batch_no, 'stream_id': record.stream_id,
                            'batch_id': record.batch_id, 'audit': segment.audit.to_obj(),
                            'encoded': segment.encoded.to_obj()})
            occurrences[side].extend(collect_occurrences(record.batch_no, segment.rows))
        matches, unmatched = plan_pairs(occurrences['left'], occurrences['right'], tolerance)
        return {'version': 2, 'snapshot_id': snapshot_id,
                'left_stream': left_stream, 'right_stream': right_stream,
                'tolerance': tolerance,
                'sources': {'version': 1, 'snapshot_id': snapshot_id, 'batches': batches},
                'matches': matches, 'unmatched': unmatched}
