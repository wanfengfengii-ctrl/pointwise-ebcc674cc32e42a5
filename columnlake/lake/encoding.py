from dataclasses import dataclass
from copy import deepcopy
import json
from columnlake.source.events import DataError, exact_int
from columnlake.source.schema import parse_schema, parse_type
from columnlake.source.identity import canonical_json
from columnlake.assembly.structure import validate_validity, validate_struct_layout, validate_offsets
from columnlake.assembly.batches import freeze_batch


@dataclass(frozen=True)
class EncodedBatch:
    payload: str

    def to_obj(self):
        return json.loads(self.payload)


def encode_batch(batch):
    freeze_batch(batch)
    local = {generation.key: number for number, generation in enumerate(batch.closure)}

    def column(t, cells):
        cells = tuple(cells)
        result = {'length': len(cells), 'validity': [cell is not None for cell in cells]}
        if t.kind in ('int', 'text'):
            placeholder = 0 if t.kind == 'int' else ''
            result['values'] = [placeholder if cell is None else cell for cell in cells]
        elif t.kind == 'dict':
            result['indices'] = [0 if cell is None else cell.index for cell in cells]
            result['references'] = [0 if cell is None else local[cell.generation.key] for cell in cells]
        elif t.kind == 'struct':
            result['fields'] = {name: column(child, [None if cell is None else cell[i] for cell in cells])
                                for i, (name, child) in enumerate(t.fields)}
        else:
            offsets = [0]
            children = []
            for cell in cells:
                if cell is not None:
                    children.extend(cell)
                offsets.append(len(children))
            result['offsets'] = offsets
            result['items'] = column(t.item, children)
        return result

    columns = {name: column(t, [row[i] for row in batch.bound_rows])
               for i, (name, t) in enumerate(batch.schema.fields)}
    dictionaries = [{'id': local[g.key], 'type': g.value_type.to_obj(),
                     'array': column(g.value_type, g.values)} for g in batch.closure]
    value = {'version': 1, 'schema': batch.schema.to_obj(), 'row_count': batch.audit.row_count,
             'columns': columns, 'dictionaries': dictionaries}
    return EncodedBatch(canonical_json(value))


def decode_batch(encoded):
    if isinstance(encoded, EncodedBatch):
        raw = encoded.to_obj()
    elif type(encoded) is str:
        raw = json.loads(encoded)
    else:
        raw = encoded
    if type(raw) is not dict or set(raw) != {'version', 'schema', 'row_count', 'columns', 'dictionaries'}:
        raise DataError('invalid encoded batch')
    if type(raw['version']) is not int or raw['version'] != 1 or not exact_int(raw['row_count']):
        raise DataError('unsupported encoded batch version or length')
    schema = parse_schema(raw['schema'])
    entries = {}
    if type(raw['dictionaries']) is not list:
        raise DataError('invalid local dictionary directory')
    for entry in raw['dictionaries']:
        if type(entry) is not dict or set(entry) != {'id', 'type', 'array'}:
            raise DataError('invalid local dictionary')
        number = entry['id']
        if not exact_int(number) or number in entries:
            raise DataError('duplicate local dictionary')
        entries[number] = entry
    cache = {}
    active = set()

    def dictionary(number):
        if not exact_int(number) or number not in entries or number in active:
            raise DataError('missing or cyclic local dictionary')
        if number not in cache:
            active.add(number)
            entry = entries[number]
            t = parse_type(entry['type'])
            values = array(t, entry['array'])
            active.remove(number)
            cache[number] = (t, values)
        return cache[number]

    def array(t, value):
        if type(value) is not dict or 'length' not in value or 'validity' not in value:
            raise DataError('invalid encoded array')
        n = value['length']
        valid = validate_validity(value['validity'], n)
        extra = {'int': {'values'}, 'text': {'values'}, 'dict': {'indices', 'references'},
                 'struct': {'fields'}, 'list': {'offsets', 'items'}}[t.kind]
        if set(value) != {'length', 'validity'} | extra:
            raise DataError('encoded array keys do not match type')
        if t.kind in ('int', 'text'):
            values = value['values']
            if type(values) is not list or len(values) != n:
                raise DataError('encoded scalar length mismatch')
            expected = int if t.kind == 'int' else str
            if any(ok and type(cell) is not expected for ok, cell in zip(valid, values)):
                raise DataError('encoded scalar type mismatch')
            return [cell if ok else None for ok, cell in zip(valid, values)]
        if t.kind == 'dict':
            indices, refs = value['indices'], value['references']
            if type(indices) is not list or type(refs) is not list or len(indices) != n or len(refs) != n:
                raise DataError('encoded reference length mismatch')
            result = []
            for ok, index, ref in zip(valid, indices, refs):
                if not ok:
                    result.append(None)
                    continue
                value_type, values = dictionary(ref)
                if value_type != t.item or not exact_int(index) or index >= len(values):
                    raise DataError('invalid encoded dictionary index or type')
                result.append(deepcopy(values[index]))
            return result
        if t.kind == 'struct':
            validate_struct_layout(value['fields'], t.fields, n)
            children = {name: array(child, value['fields'][name]) for name, child in t.fields}
            return [{name: children[name][i] for name, _ in t.fields} if valid[i] else None
                    for i in range(n)]
        items = value['items']
        if type(items) is not dict or 'length' not in items:
            raise DataError('invalid encoded list child')
        offsets = validate_offsets(value['offsets'], n, items['length'])
        values = array(t.item, items)
        return [values[offsets[i]:offsets[i + 1]] if valid[i] else None for i in range(n)]

    for number in entries:
        dictionary(number)
    columns = raw['columns']
    if type(columns) is not dict or set(columns) != {name for name, _ in schema.fields}:
        raise DataError('encoded columns do not match schema')
    decoded = {name: array(t, columns[name]) for name, t in schema.fields}
    if any(len(values) != raw['row_count'] for values in decoded.values()):
        raise DataError('encoded row count mismatch')
    rows = [{name: decoded[name][i] for name, _ in schema.fields} for i in range(raw['row_count'])]
    return schema, rows


def validate_encoding(encoded):
    decode_batch(encoded)
