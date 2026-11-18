from dataclasses import dataclass
import hashlib
from columnlake.source.events import DataError, exact_int
from columnlake.source.identity import canonical_json


@dataclass(frozen=True)
class BatchAudit:
    row_count: int
    digest: str
    leaf_values: int
    nulls: int

    def to_obj(self):
        return {'row_count': self.row_count, 'digest': self.digest,
                'leaf_values': self.leaf_values, 'nulls': self.nulls}

    @classmethod
    def from_obj(cls, value):
        if type(value) is not dict or set(value) != {'row_count', 'digest', 'leaf_values', 'nulls'}:
            raise DataError('invalid audit record')
        if any(not exact_int(value[key]) for key in ('row_count', 'leaf_values', 'nulls')):
            raise DataError('invalid audit counts')
        digest = value['digest']
        if type(digest) is not str or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
            raise DataError('invalid audit digest')
        return cls(value['row_count'], digest, value['leaf_values'], value['nulls'])


def audit_rows(schema, rows):
    leaves = 0
    nulls = 0

    def tag(t, value):
        nonlocal leaves, nulls
        if value is None:
            nulls += 1
            return ['null']
        if t.kind == 'dict':
            return tag(t.item, value)
        if t.kind == 'struct':
            if type(value) is not dict or set(value) != {n for n, _ in t.fields}:
                raise DataError('logical struct does not match schema')
            return ['struct', [[n, tag(c, value[n])] for n, c in t.fields]]
        if t.kind == 'list':
            if type(value) is not list:
                raise DataError('logical list expected')
            return ['list', [tag(t.item, cell) for cell in value]]
        if type(value) is not (int if t.kind == 'int' else str):
            raise DataError('logical scalar type mismatch')
        leaves += 1
        return [t.kind, str(value) if t.kind == 'int' else value]

    encoded_rows = [tag(schema.root, row) for row in rows]
    document = {'format': 1, 'schema': schema.to_obj(), 'rows': encoded_rows}
    digest = hashlib.sha256(canonical_json(document).encode('utf-8')).hexdigest()
    return BatchAudit(len(rows), digest, leaves, nulls)


def verify_audit(schema, rows, expected):
    if audit_rows(schema, rows) != expected:
        raise DataError('logical batch audit mismatch')
