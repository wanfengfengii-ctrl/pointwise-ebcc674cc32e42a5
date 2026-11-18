from dataclasses import dataclass
from columnlake.source.events import DataError, exact_int


@dataclass(frozen=True)
class TypeSpec:
    kind: str
    fields: tuple = ()
    item: object = None
    dictionary_id: object = None

    def to_obj(self):
        if self.kind in ('int', 'text'):
            return self.kind
        if self.kind == 'struct':
            return {'struct': [[name, child.to_obj()] for name, child in self.fields]}
        if self.kind == 'list':
            return {'list': self.item.to_obj()}
        if self.kind == 'dict':
            return {'dict': {'id': self.dictionary_id, 'value': self.item.to_obj()}}
        raise DataError('unsupported type')


@dataclass(frozen=True)
class Schema:
    fields: tuple

    @property
    def root(self):
        return TypeSpec('struct', fields=self.fields)

    def to_obj(self):
        return [[name, child.to_obj()] for name, child in self.fields]


def _fields(raw):
    if type(raw) is not list:
        raise DataError('fields must be an ordered list')
    result = []
    names = set()
    for field in raw:
        if type(field) is not list or len(field) != 2:
            raise DataError('field must be [name, type]')
        name, value = field
        if type(name) is not str or not name or name in names:
            raise DataError('invalid or duplicate field name')
        names.add(name)
        result.append((name, parse_type(value)))
    return tuple(result)


def parse_type(raw):
    if isinstance(raw, TypeSpec):
        raw = raw.to_obj()
    if type(raw) is str and raw in ('int', 'text'):
        return TypeSpec(raw)
    if type(raw) is not dict or len(raw) != 1:
        raise DataError('invalid type tree')
    if 'struct' in raw:
        return TypeSpec('struct', fields=_fields(raw['struct']))
    if 'list' in raw:
        return TypeSpec('list', item=parse_type(raw['list']))
    if 'dict' in raw:
        body = raw['dict']
        if type(body) is not dict or set(body) != {'id', 'value'}:
            raise DataError('invalid dictionary type')
        if not exact_int(body['id']):
            raise DataError('invalid dictionary number')
        return TypeSpec('dict', item=parse_type(body['value']), dictionary_id=body['id'])
    raise DataError('unsupported type')


def parse_schema(raw):
    if isinstance(raw, Schema):
        raw = raw.to_obj()
    return Schema(_fields(raw))


def logical_type(t):
    if t.kind == 'dict':
        return logical_type(t.item)
    if t.kind == 'list':
        return TypeSpec('list', item=logical_type(t.item))
    if t.kind == 'struct':
        return TypeSpec('struct', fields=tuple((n, logical_type(c)) for n, c in t.fields))
    return t


def path_tokens(path):
    tokens = path.split('.') if type(path) is str else path
    if not isinstance(tokens, (tuple, list)) or not tokens:
        raise DataError('path must contain field names')
    if any(type(part) is not str or not part for part in tokens):
        raise DataError('invalid path')
    return tuple(tokens)


def resolve_type(schema, path):
    tokens = path_tokens(path)

    def descend(t, remaining):
        if not remaining:
            return logical_type(t)
        if t.kind == 'dict':
            return descend(t.item, remaining)
        if t.kind == 'list':
            return TypeSpec('list', item=descend(t.item, remaining))
        if t.kind != 'struct':
            raise DataError('path crosses a scalar')
        children = dict(t.fields)
        if remaining[0] not in children:
            raise DataError('unknown field in path')
        return descend(children[remaining[0]], remaining[1:])

    return descend(parse_schema(schema).root, tokens)
