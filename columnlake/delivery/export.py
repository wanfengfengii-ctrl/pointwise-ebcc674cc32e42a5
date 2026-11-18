from dataclasses import dataclass
import json
from columnlake.source.identity import canonical_json
from columnlake.delivery.reader import SnapshotReader


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
