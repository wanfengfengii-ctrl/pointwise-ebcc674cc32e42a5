from copy import deepcopy
import hashlib
import json
from columnlake.source.events import DataError, exact_int
from columnlake.source.schema import parse_schema, parse_type
from columnlake.assembly.structure import validate_validity, validate_struct_layout, validate_offsets
from columnlake.delivery.pairing import require_pair_schema, collect_occurrences


def decode_export(bundle):
    try:
        if hasattr(bundle, 'to_obj'):
            bundle = bundle.to_obj()
        elif type(bundle) is str:
            bundle = json.loads(bundle)
        if type(bundle) is not dict:
            raise DataError('invalid export envelope')
        version = bundle.get('version')
        if type(version) is not int or version not in (1, 2):
            raise DataError('unsupported export version')
        if version == 1:
            rows = []
            for _, _, member_rows in _decode_v1_members(bundle):
                rows.extend(member_rows)
            return rows
        return _decode_pair_delivery(bundle)
    except DataError:
        raise
    except (ValueError, TypeError, KeyError, IndexError, RecursionError) as exc:
        raise DataError('malformed training export') from exc


def _decode_v1_members(bundle):
    """验证V1来源的结构、引用图、列布局与审计，返回(member, schema, rows)列表。"""
    if set(bundle) != {'version', 'snapshot_id', 'batches'}:
        raise DataError('invalid export envelope')
    if type(bundle['version']) is not int or bundle['version'] != 1 \
            or not exact_int(bundle['snapshot_id']):
        raise DataError('unsupported export version or snapshot')
    if type(bundle['batches']) is not list:
        raise DataError('invalid export batch directory')
    members = []
    previous_number = 0
    identities = set()
    for member in bundle['batches']:
        keys = {'batch_no', 'stream_id', 'batch_id', 'audit', 'encoded'}
        if type(member) is not dict or set(member) != keys:
            raise DataError('invalid export member')
        number = member['batch_no']
        if not exact_int(number, 1) or number <= previous_number:
            raise DataError('export batch order is invalid')
        previous_number = number
        if any(type(member[k]) is not str or not member[k] for k in ('stream_id', 'batch_id')):
            raise DataError('invalid exported source identity')
        identity = (member['stream_id'], member['batch_id'])
        if identity in identities:
            raise DataError('duplicate exported source batch')
        identities.add(identity)
        raw = member['encoded']
        if type(raw) is not dict or set(raw) != {'version', 'schema', 'row_count', 'columns', 'dictionaries'}:
            raise DataError('invalid exported column batch')
        if type(raw['version']) is not int or raw['version'] != 1 or not exact_int(raw['row_count']):
            raise DataError('invalid exported column version or length')
        schema = parse_schema(raw['schema'])
        if type(raw['dictionaries']) is not list:
            raise DataError('invalid exported dictionaries')
        definitions = {}
        for definition in raw['dictionaries']:
            if type(definition) is not dict or set(definition) != {'id', 'type', 'array'}:
                raise DataError('invalid exported dictionary entry')
            local_id = definition['id']
            if not exact_int(local_id) or local_id in definitions:
                raise DataError('duplicate exported local identity')
            definitions[local_id] = definition
        decoded_dictionaries = {}
        resolving = set()

        def resolve(local_id):
            if not exact_int(local_id) or local_id not in definitions or local_id in resolving:
                raise DataError('missing or cyclic exported dictionary')
            if local_id not in decoded_dictionaries:
                resolving.add(local_id)
                definition = definitions[local_id]
                value_type = parse_type(definition['type'])
                values = expand(value_type, definition['array'])
                resolving.remove(local_id)
                decoded_dictionaries[local_id] = value_type, values
            return decoded_dictionaries[local_id]

        def expand(t, column):
            if type(column) is not dict or 'length' not in column or 'validity' not in column:
                raise DataError('invalid exported array')
            size = column['length']
            validity = validate_validity(column['validity'], size)
            required = {'int': {'values'}, 'text': {'values'}, 'dict': {'indices', 'references'},
                        'struct': {'fields'}, 'list': {'offsets', 'items'}}[t.kind]
            if set(column) != {'length', 'validity'} | required:
                raise DataError('exported array shape mismatch')
            if t.kind in ('int', 'text'):
                values = column['values']
                if type(values) is not list or len(values) != size:
                    raise DataError('exported scalar length mismatch')
                result = []
                for position in range(size):
                    cell = values[position]
                    if validity[position] and type(cell) is not (int if t.kind == 'int' else str):
                        raise DataError('exported scalar type mismatch')
                    result.append(cell if validity[position] else None)
                return result
            if t.kind == 'dict':
                indices = column['indices']
                references = column['references']
                if (type(indices) is not list or type(references) is not list
                        or len(indices) != size or len(references) != size):
                    raise DataError('exported reference length mismatch')
                result = []
                for position in range(size):
                    if not validity[position]:
                        result.append(None)
                        continue
                    value_type, values = resolve(references[position])
                    index = indices[position]
                    if value_type != t.item or not exact_int(index) or index >= len(values):
                        raise DataError('exported reference type or index mismatch')
                    result.append(deepcopy(values[index]))
                return result
            if t.kind == 'struct':
                validate_struct_layout(column['fields'], t.fields, size)
                children = [(name, expand(child, column['fields'][name])) for name, child in t.fields]
                return [{name: cells[i] for name, cells in children} if validity[i] else None
                        for i in range(size)]
            child_column = column['items']
            if type(child_column) is not dict or 'length' not in child_column:
                raise DataError('invalid exported list child')
            offsets = validate_offsets(column['offsets'], size, child_column['length'])
            children = expand(t.item, child_column)
            return [children[offsets[i]:offsets[i + 1]] if validity[i] else None
                    for i in range(size)]

        for local_id in definitions:
            resolve(local_id)
        if type(raw['columns']) is not dict or set(raw['columns']) != {n for n, _ in schema.fields}:
            raise DataError('exported schema/column mismatch')
        columns = [(name, expand(t, raw['columns'][name])) for name, t in schema.fields]
        if any(len(cells) != raw['row_count'] for _, cells in columns):
            raise DataError('exported row count mismatch')
        rows = [{name: cells[i] for name, cells in columns} for i in range(raw['row_count'])]
        leaves = 0
        nulls = 0

        def tagged(t, value):
            nonlocal leaves, nulls
            if value is None:
                nulls += 1
                return ['null']
            if t.kind == 'dict':
                return tagged(t.item, value)
            if t.kind == 'struct':
                return ['struct', [[name, tagged(child, value[name])] for name, child in t.fields]]
            if t.kind == 'list':
                return ['list', [tagged(t.item, cell) for cell in value]]
            leaves += 1
            return [t.kind, str(value) if t.kind == 'int' else value]

        audit_document = {'format': 1, 'schema': schema.to_obj(),
                          'rows': [tagged(schema.root, row) for row in rows]}
        text = json.dumps(audit_document, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False)
        measured = {'row_count': len(rows), 'digest': hashlib.sha256(text.encode('utf-8')).hexdigest(),
                    'leaf_values': leaves, 'nulls': nulls}
        if member['audit'] != measured:
            raise DataError('independent export audit mismatch')
        members.append((member, schema, rows))
    return members


def _address(value):
    if type(value) is not list or len(value) != 3 or any(not exact_int(part) for part in value):
        raise DataError('invalid occurrence address')
    return tuple(value)


def _decode_pair_delivery(bundle):
    keys = {'version', 'snapshot_id', 'left_stream', 'right_stream',
            'tolerance', 'sources', 'matches', 'unmatched'}
    if set(bundle) != keys:
        raise DataError('invalid pair export envelope')
    snapshot_id = bundle['snapshot_id']
    left_stream = bundle['left_stream']
    right_stream = bundle['right_stream']
    tolerance = bundle['tolerance']
    if not exact_int(snapshot_id):
        raise DataError('invalid pair snapshot identity')
    if type(left_stream) is not str or not left_stream \
            or type(right_stream) is not str or not right_stream \
            or left_stream == right_stream:
        raise DataError('invalid pair streams')
    if not exact_int(tolerance):
        raise DataError('invalid pair tolerance')
    sources = bundle['sources']
    if type(sources) is not dict:
        raise DataError('invalid pair sources')
    # 先完整验证来源的V1结构和审计，再解释关系。
    members = _decode_v1_members(sources)
    if sources['snapshot_id'] != snapshot_id:
        raise DataError('pair sources snapshot mismatch')
    occurrences = {'left': {}, 'right': {}}
    for member, schema, rows in members:
        if member['stream_id'] == left_stream:
            side = 'left'
        elif member['stream_id'] == right_stream:
            side = 'right'
        else:
            raise DataError('pair source stream mismatch')
        require_pair_schema(schema)
        for occ in collect_occurrences(member['batch_no'], rows):
            occurrences[side][occ.address] = occ
    matches = bundle['matches']
    unmatched = bundle['unmatched']
    if type(matches) is not list or type(unmatched) is not list:
        raise DataError('invalid pair relations')
    consumed = {'left': set(), 'right': set()}
    pairs = []
    previous_left = None
    for entry in matches:
        if type(entry) is not list or len(entry) != 2:
            raise DataError('invalid pair entry')
        left_address = _address(entry[0])
        right_address = _address(entry[1])
        if previous_left is not None and left_address <= previous_left:
            raise DataError('pair entries are not ordered by left address')
        previous_left = left_address
        left = occurrences['left'].get(left_address)
        right = occurrences['right'].get(right_address)
        if left is None or right is None:
            raise DataError('pair address outside source occurrences')
        if left_address in consumed['left'] or right_address in consumed['right']:
            raise DataError('pair consumes an occurrence twice')
        consumed['left'].add(left_address)
        consumed['right'].add(right_address)
        if left.sample is None or left.category is None or left.uv is None:
            raise DataError('pair left occurrence is not eligible')
        if right.sample is None or right.category is None or right.uv is None:
            raise DataError('pair right occurrence is not eligible')
        if left.sample != right.sample or left.category != right.category:
            raise DataError('pair group mismatch')
        if abs(left.uv - right.uv) > tolerance:
            raise DataError('pair exceeds tolerance')
        pairs.append({'left': list(left_address), 'right': list(right_address),
                      'sample': left.sample, 'category': left.category,
                      'left_uv': left.uv, 'right_uv': right.uv,
                      'delta_uv': right.uv - left.uv})
    listed = {'left': set(), 'right': set()}
    unmatched_rows = []
    previous_key = None
    for entry in unmatched:
        if type(entry) is not list or len(entry) != 2:
            raise DataError('invalid unmatched entry')
        side, raw_address = entry
        if side not in ('left', 'right'):
            raise DataError('invalid unmatched side')
        address = _address(raw_address)
        occ = occurrences[side].get(address)
        if occ is None:
            raise DataError('unmatched address outside source occurrences')
        key = (0 if side == 'left' else 1, address)
        if previous_key is not None and key <= previous_key:
            raise DataError('unmatched entries are not ordered')
        previous_key = key
        if address in consumed[side]:
            raise DataError('occurrence is both paired and unmatched')
        listed[side].add(address)
        unmatched_rows.append({'side': side, 'address': list(address),
                               'sample': occ.sample, 'observation': occ.item})
    for side in ('left', 'right'):
        if consumed[side] | listed[side] != set(occurrences[side]):
            raise DataError('pair delivery does not cover all source occurrences')
    return {'pairs': pairs, 'unmatched': unmatched_rows}


def validate_export(bundle):
    decode_export(bundle)
