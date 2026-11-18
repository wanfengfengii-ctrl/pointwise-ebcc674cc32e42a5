from dataclasses import dataclass
from columnlake.source.events import DataError
from columnlake.delivery.projection import compile_projection, project_rows
from columnlake.delivery.reader import SnapshotReader


@dataclass(frozen=True)
class CategoryProfile:
    counts: tuple
    nulls: int

    def to_obj(self):
        return {'counts': [{'type': kind, 'value': value, 'count': count}
                           for kind, value, count in self.counts], 'nulls': self.nulls}


def _leaf(t):
    while t.kind == 'list':
        t = t.item
    if t.kind not in ('int', 'text'):
        raise DataError('profile path must end in an integer or text leaf')
    return t


def count_occurrences(t, values):
    _leaf(t)
    counts = {}
    nulls = 0

    def visit(current_type, value):
        nonlocal nulls
        if value is None:
            nulls += 1
        elif current_type.kind == 'list':
            for child in value:
                visit(current_type.item, child)
        else:
            key = (current_type.kind, value)
            counts[key] = counts.get(key, 0) + 1

    for value in values:
        visit(t, value)
    return CategoryProfile(tuple((kind, value, n) for (kind, value), n in counts.items()), nulls)


def profile(store, snapshot_id, categorical_path):
    batches = list(SnapshotReader(store).iter_batches(snapshot_id))
    if not batches:
        raise DataError('cannot resolve a profile path on an empty snapshot')
    plans = [compile_projection(segment.schema, categorical_path) for _, segment in batches]
    for plan in plans:
        _leaf(plan.result_type)
    counts = {}
    nulls = 0
    for (_, segment), plan in zip(batches, plans, strict=True):
        part = count_occurrences(plan.result_type, project_rows(plan, segment.rows))
        nulls += part.nulls
        for kind, value, n in part.counts:
            counts[(kind, value)] = counts.get((kind, value), 0) + n
    return CategoryProfile(tuple((kind, value, n) for (kind, value), n in counts.items()), nulls)
