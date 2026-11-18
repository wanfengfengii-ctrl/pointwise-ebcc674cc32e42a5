from columnlake.source.events import DataError
from columnlake.dictionary.references import FrozenReference


def interpret_value(t, value):
    if value is None:
        return None
    if t.kind == 'dict':
        if not isinstance(value, FrozenReference) or value.generation.value_type != t.item:
            raise DataError('unbound dictionary interpretation')
        return interpret_value(t.item, value.generation.values[value.index])
    if t.kind == 'struct':
        return {name: interpret_value(child, cell)
                for (name, child), cell in zip(t.fields, value, strict=True)}
    if t.kind == 'list':
        return [interpret_value(t.item, cell) for cell in value]
    return value


def interpret_rows(schema, rows):
    return [interpret_value(schema.root, row) for row in rows]
