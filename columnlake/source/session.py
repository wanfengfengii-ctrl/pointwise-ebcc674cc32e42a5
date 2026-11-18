from dataclasses import dataclass
from columnlake.source.events import DataError, DictionaryEvent, BatchEvent, exact_int
from columnlake.source.schema import parse_schema
from columnlake.source.validation import EventCursor
from columnlake.dictionary.catalog import DictionaryCatalog
from columnlake.assembly.arrays import extract_cells
from columnlake.assembly.batches import assemble_batch


@dataclass(frozen=True)
class StreamState:
    stream_id: str
    schema: object
    next_seq: int
    ended: bool
    dictionaries: object
    batch_ids: tuple

    @classmethod
    def empty(cls, stream_id):
        return cls(stream_id, None, 0, False, DictionaryCatalog(stream_id), ())

    def to_obj(self):
        return {'stream_id': self.stream_id,
                'schema': None if self.schema is None else self.schema.to_obj(),
                'next_seq': self.next_seq, 'ended': self.ended,
                'dictionaries': self.dictionaries.export_state(), 'batch_ids': list(self.batch_ids)}

    @classmethod
    def from_obj(cls, raw):
        expected = {'stream_id', 'schema', 'next_seq', 'ended', 'dictionaries', 'batch_ids'}
        if type(raw) is not dict or set(raw) != expected:
            raise DataError('invalid stored stream state')
        if type(raw['stream_id']) is not str or not raw['stream_id']:
            raise DataError('invalid stored stream identity')
        if not exact_int(raw['next_seq']) or type(raw['ended']) is not bool:
            raise DataError('invalid stored event cursor')
        ids = raw['batch_ids']
        if type(ids) is not list or any(type(x) is not str or not x for x in ids) or len(set(ids)) != len(ids):
            raise DataError('invalid stored batch directory')
        catalog = DictionaryCatalog.from_state(raw['dictionaries'])
        if catalog.stream_id != raw['stream_id']:
            raise DataError('stream/catalog identity mismatch')
        schema = None if raw['schema'] is None else parse_schema(raw['schema'])
        if schema is None or raw['next_seq'] == 0:
            raise DataError('committed stream must have a schema')
        return cls(raw['stream_id'], schema, raw['next_seq'], raw['ended'], catalog, tuple(ids))


@dataclass(frozen=True)
class PreparedIntake:
    state: StreamState
    batches: tuple


class IngestSession:
    def __init__(self, state):
        self.state = state

    def prepare(self, events):
        cursor = EventCursor(self.state.schema, self.state.next_seq, self.state.ended)
        dictionaries = self.state.dictionaries.clone()
        batch_ids = list(self.state.batch_ids)
        batches = []
        for event in events:
            cursor.accept(event)
            if isinstance(event, DictionaryEvent):
                cells = extract_cells(event.value_type, event.values)
                operation = dictionaries.apply_delta if event.delta else dictionaries.apply_replace
                operation(event.dictionary_id, event.value_type, cells)
            elif isinstance(event, BatchEvent):
                batch = assemble_batch(self.state.stream_id, cursor.schema, event,
                                       dictionaries, batch_ids)
                batches.append(batch)
                batch_ids.append(event.batch_id)
        if cursor.schema is None:
            raise DataError('request did not establish a schema')
        state = StreamState(self.state.stream_id, cursor.schema, cursor.next_seq,
                            cursor.ended, dictionaries, tuple(batch_ids))
        return PreparedIntake(state, tuple(batches))
