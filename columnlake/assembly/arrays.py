from dataclasses import dataclass
from columnlake.source.events import DataError
from columnlake.assembly.structure import (
    validate_validity, validate_struct_layout, validate_offsets,
)
from columnlake.dictionary.generations import EncodedIndex


@dataclass(frozen=True)
class ColumnArray:
    type_spec: object
    cells: tuple

    @property
    def length(self):
        return len(self.cells)


def extract_cells(t, array):
    if type(array) is not dict or 'length' not in array or 'validity' not in array:
        raise DataError('array requires length and validity')
    length = array['length']
    validity = validate_validity(array['validity'], length)
    payload_keys = {'int': {'values'}, 'text': {'values'}, 'dict': {'indices'},
                    'struct': {'fields'}, 'list': {'offsets', 'items'}}
    if set(array) != {'length', 'validity'} | payload_keys[t.kind]:
        raise DataError('array payload does not match type')
    if t.kind in ('int', 'text', 'dict'):
        payload = array['indices' if t.kind == 'dict' else 'values']
        if type(payload) is not list or len(payload) != length:
            raise DataError('scalar payload length mismatch')
        cells = []
        for valid, value in zip(validity, payload, strict=True):
            if not valid:
                cells.append(None)
                continue
            expected = str if t.kind == 'text' else int
            if type(value) is not expected:
                raise DataError('scalar payload type mismatch')
            cells.append(EncodedIndex(value) if t.kind == 'dict' else value)
        return tuple(cells)
    if t.kind == 'struct':
        validate_struct_layout(array['fields'], t.fields, length)
        children = [extract_cells(child, array['fields'][name]) for name, child in t.fields]
        return tuple(tuple(child[row] for child in children) if validity[row] else None
                     for row in range(length))
    items = array['items']
    if type(items) is not dict or 'length' not in items:
        raise DataError('list requires a child array')
    offsets = validate_offsets(array['offsets'], length, items['length'])
    child = extract_cells(t.item, items)
    return tuple(tuple(child[offsets[row]:offsets[row + 1]]) if validity[row] else None
                 for row in range(length))


def validate_array(t, array):
    return ColumnArray(t, extract_cells(t, array))
