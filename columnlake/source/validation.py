from dataclasses import dataclass
from columnlake.source.events import (
    DataError, SchemaEvent, DictionaryEvent, BatchEvent, EndEvent, exact_int,
)
from columnlake.source.schema import parse_schema


def validate_event_envelope(event, expected_seq):
    if not exact_int(event.seq) or event.seq != expected_seq:
        raise DataError('event sequence must be continuous')


def validate_batch_envelope(event, schema):
    if schema is None or type(event.columns) is not dict:
        raise DataError('batch requires a schema and columns')
    if set(event.columns) != {name for name, _ in schema.fields}:
        raise DataError('batch columns do not match schema')
    if not exact_int(event.row_count):
        raise DataError('invalid row count')
    if type(event.batch_id) is not str or not event.batch_id:
        raise DataError('invalid batch identity')


@dataclass
class EventCursor:
    schema: object = None
    next_seq: int = 0
    ended: bool = False

    def accept(self, event):
        validate_event_envelope(event, self.next_seq)
        if self.ended:
            raise DataError('stream has ended')
        if isinstance(event, SchemaEvent):
            if self.schema is not None:
                raise DataError('schema is already fixed')
            self.schema = parse_schema(event.schema)
        else:
            if self.schema is None:
                raise DataError('schema must be the first event')
            if isinstance(event, BatchEvent):
                validate_batch_envelope(event, self.schema)
            elif isinstance(event, EndEvent):
                self.ended = True
            elif not isinstance(event, DictionaryEvent):
                raise DataError('unknown event')
        self.next_seq += 1
