from dataclasses import dataclass
import json


class DataError(ValueError):
    pass


@dataclass(frozen=True)
class SchemaEvent:
    seq: int
    schema: object


@dataclass(frozen=True)
class DictionaryEvent:
    seq: int
    dictionary_id: int
    value_type: object
    values: object
    delta: bool = False


@dataclass(frozen=True)
class BatchEvent:
    seq: int
    batch_id: str
    columns: object
    row_count: int


@dataclass(frozen=True)
class EndEvent:
    seq: int


def exact_int(value, minimum=0):
    return type(value) is int and value >= minimum


def json_copy(value):
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False))
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataError('payload must be finite JSON data') from exc


def event_to_obj(event):
    from columnlake.source.schema import parse_schema, parse_type
    if isinstance(event, SchemaEvent):
        return {'kind': 'schema', 'seq': event.seq,
                'schema': parse_schema(event.schema).to_obj()}
    if isinstance(event, DictionaryEvent):
        return {'kind': 'dictionary', 'seq': event.seq, 'id': event.dictionary_id,
                'type': parse_type(event.value_type).to_obj(),
                'values': json_copy(event.values), 'delta': event.delta}
    if isinstance(event, BatchEvent):
        return {'kind': 'batch', 'seq': event.seq, 'batch_id': event.batch_id,
                'columns': json_copy(event.columns), 'row_count': event.row_count}
    if isinstance(event, EndEvent):
        return {'kind': 'end', 'seq': event.seq}
    raise DataError('unknown typed event')


def normalize_event(event):
    from columnlake.source.schema import parse_schema, parse_type
    if isinstance(event, (SchemaEvent, DictionaryEvent, BatchEvent, EndEvent)):
        event = event_to_obj(event)
    if type(event) is not dict:
        raise DataError('event must be an object')
    kind = event.get('kind')
    fields = {
        'schema': {'kind', 'seq', 'schema'},
        'dictionary': {'kind', 'seq', 'id', 'type', 'values', 'delta'},
        'batch': {'kind', 'seq', 'batch_id', 'columns', 'row_count'},
        'end': {'kind', 'seq'},
    }
    if kind not in fields:
        raise DataError('unknown event kind')
    event = dict(event)
    if kind == 'dictionary':
        event.setdefault('delta', False)
    if set(event) != fields[kind] or not exact_int(event.get('seq')):
        raise DataError('invalid event envelope')
    seq = event['seq']
    if kind == 'schema':
        return SchemaEvent(seq, parse_schema(event['schema']))
    if kind == 'dictionary':
        if not exact_int(event['id']) or type(event['delta']) is not bool:
            raise DataError('invalid dictionary envelope')
        return DictionaryEvent(seq, event['id'], parse_type(event['type']),
                               json_copy(event['values']), event['delta'])
    if kind == 'batch':
        if not isinstance(event['batch_id'], str) or not event['batch_id']:
            raise DataError('invalid batch identity')
        if not exact_int(event['row_count']) or type(event['columns']) is not dict:
            raise DataError('invalid batch envelope')
        return BatchEvent(seq, event['batch_id'], json_copy(event['columns']),
                          event['row_count'])
    return EndEvent(seq)
