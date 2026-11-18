from dataclasses import dataclass
from columnlake.source.events import DataError
from columnlake.source.identity import canonical_json
from columnlake.assembly.arrays import validate_array
from columnlake.assembly.audit import audit_rows, verify_audit
from columnlake.dictionary.references import bind_references, dependency_closure
from columnlake.dictionary.interpretation import interpret_rows


@dataclass(frozen=True)
class LogicalBatch:
    stream_id: str
    batch_id: str
    schema: object
    physical_columns: str
    bound_rows: tuple
    closure: tuple
    audit: object

    def rows(self):
        return interpret_rows(self.schema, self.bound_rows)


def assemble_batch(stream_id, schema, event, catalog, known_batch_ids):
    if event.batch_id in known_batch_ids:
        raise DataError('source batch identity was already accepted')
    columns = []
    for name, t in schema.fields:
        column = validate_array(t, event.columns[name])
        if column.length != event.row_count:
            raise DataError('batch row count mismatch')
        columns.append(tuple(bind_references(t, cell, catalog) for cell in column.cells))
    bound_rows = tuple(tuple(column[row] for column in columns)
                       for row in range(event.row_count))
    closure = dependency_closure(bound_rows)
    logical_rows = interpret_rows(schema, bound_rows)
    batch = LogicalBatch(stream_id, event.batch_id, schema, canonical_json(event.columns),
                         bound_rows, closure, audit_rows(schema, logical_rows))
    return freeze_batch(batch)


def freeze_batch(batch):
    if dependency_closure(batch.bound_rows) != batch.closure:
        raise DataError('incomplete frozen dictionary closure')
    if any(g.key.stream_id != batch.stream_id for g in batch.closure):
        raise DataError('batch references a foreign input stream')
    verify_audit(batch.schema, batch.rows(), batch.audit)
    return batch
