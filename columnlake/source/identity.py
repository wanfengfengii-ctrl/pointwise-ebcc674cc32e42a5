from dataclasses import dataclass
import hashlib
import json
from columnlake.source.events import DataError, event_to_obj


def canonical_json(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError, OverflowError) as exc:
        raise DataError('value has no canonical JSON representation') from exc


@dataclass(frozen=True)
class RequestIdentity:
    content: str
    digest: str


def canonical_request(stream_id, events):
    if type(stream_id) is not str or not stream_id:
        raise DataError('stream_id must be nonempty text')
    return canonical_json({'stream_id': stream_id,
                           'events': [event_to_obj(event) for event in events]})


def identify_request(stream_id, events):
    content = canonical_request(stream_id, events)
    return RequestIdentity(content, hashlib.sha256(content.encode('utf-8')).hexdigest())


def same_request(left, right):
    return left.content == right.content
