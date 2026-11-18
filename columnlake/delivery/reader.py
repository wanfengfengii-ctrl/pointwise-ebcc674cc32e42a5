from columnlake.source.events import DataError
from columnlake.lake.snapshots import load_snapshot
from columnlake.lake.segments import read_segment
from columnlake.delivery.projection import compile_projection, project_rows


class SnapshotReader:
    def __init__(self, store):
        self.store = store

    def iter_batches(self, snapshot_id):
        snapshot = load_snapshot(self.store, snapshot_id)
        for record in snapshot.members:
            segment = read_segment(self.store, record.segment_id, record.audit)
            yield record, segment

    def read(self, snapshot_id, projection=None):
        batches = list(self.iter_batches(snapshot_id))
        if projection is None:
            return [row for _, segment in batches for row in segment.rows]
        if not batches:
            raise DataError('cannot resolve a path on an empty snapshot')
        plans = [compile_projection(segment.schema, projection) for _, segment in batches]
        result = []
        for (_, segment), plan in zip(batches, plans, strict=True):
            result.extend(project_rows(plan, segment.rows))
        return result
