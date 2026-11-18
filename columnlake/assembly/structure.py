from columnlake.source.events import DataError, exact_int


def validate_validity(validity, length):
    if not exact_int(length):
        raise DataError('invalid array length')
    if type(validity) is not list or len(validity) != length:
        raise DataError('validity length mismatch')
    if any(type(value) is not bool for value in validity):
        raise DataError('validity entries must be booleans')
    return tuple(validity)


def validate_struct_layout(fields, expected_fields, length):
    if type(fields) is not dict or set(fields) != {name for name, _ in expected_fields}:
        raise DataError('struct fields do not match schema')
    for name, _ in expected_fields:
        child = fields[name]
        if type(child) is not dict or child.get('length') != length:
            raise DataError('struct child length mismatch')


def validate_offsets(offsets, length, child_length):
    if type(offsets) is not list or len(offsets) != length + 1:
        raise DataError('list offset count mismatch')
    if any(not exact_int(value) for value in offsets):
        raise DataError('list offsets must be nonnegative integers')
    if not offsets or offsets[0] != 0 or offsets[-1] != child_length:
        raise DataError('list offsets must span the child array')
    if any(left > right for left, right in zip(offsets, offsets[1:])):
        raise DataError('list offsets must be monotonic')
    return tuple(offsets)
