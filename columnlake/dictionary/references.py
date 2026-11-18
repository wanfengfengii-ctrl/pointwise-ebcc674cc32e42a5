from dataclasses import dataclass
from columnlake.source.events import DataError, exact_int
from columnlake.dictionary.generations import EncodedIndex, GenerationKey


@dataclass(frozen=True)
class FrozenReference:
    generation: object
    index: int


def checked_reference(t, generation, index):
    if generation.value_type != t.item or generation.key.dictionary_id != t.dictionary_id:
        raise DataError('dictionary value type does not match reference')
    if not exact_int(index) or index >= len(generation.values):
        raise DataError('dictionary index out of range')
    return FrozenReference(generation, index)


def bind_references(t, value, catalog):
    if value is None:
        return None
    if t.kind == 'dict':
        if not isinstance(value, EncodedIndex):
            raise DataError('dictionary cell requires an encoded index')
        return checked_reference(t, catalog.lookup(t.dictionary_id), value.index)
    if t.kind == 'struct':
        return tuple(bind_references(child, cell, catalog)
                     for (_, child), cell in zip(t.fields, value, strict=True))
    if t.kind == 'list':
        return tuple(bind_references(t.item, cell, catalog) for cell in value)
    return value


def dependency_closure(values):
    found = {}
    active = set()

    def visit(value):
        if isinstance(value, FrozenReference):
            generation = value.generation
            key = generation.key
            if key in active:
                raise DataError('cyclic dictionary graph')
            if key not in found:
                active.add(key)
                for cell in generation.values:
                    visit(cell)
                active.remove(key)
                found[key] = generation
        elif isinstance(value, tuple):
            for child in value:
                visit(child)

    visit(tuple(values))
    return tuple(found[key] for key in sorted(found))


def dump_cell(t, value):
    if value is None:
        return None
    if t.kind == 'dict':
        return {'key': value.generation.key.to_obj(), 'index': value.index}
    if t.kind == 'struct':
        return [dump_cell(child, cell)
                for (_, child), cell in zip(t.fields, value, strict=True)]
    if t.kind == 'list':
        return [dump_cell(t.item, cell) for cell in value]
    return value


def load_cell(t, value, lookup):
    if value is None:
        return None
    if t.kind == 'dict':
        if type(value) is not dict or set(value) != {'key', 'index'}:
            raise DataError('invalid stored reference')
        return checked_reference(t, lookup(GenerationKey.from_obj(value['key'])), value['index'])
    if t.kind == 'struct':
        if type(value) is not list or len(value) != len(t.fields):
            raise DataError('invalid stored struct')
        return tuple(load_cell(child, cell, lookup)
                     for (_, child), cell in zip(t.fields, value, strict=True))
    if t.kind == 'list':
        if type(value) is not list:
            raise DataError('invalid stored list')
        return tuple(load_cell(t.item, cell, lookup) for cell in value)
    if t.kind == 'int' and type(value) is not int:
        raise DataError('invalid stored integer')
    if t.kind == 'text' and type(value) is not str:
        raise DataError('invalid stored text')
    return value
