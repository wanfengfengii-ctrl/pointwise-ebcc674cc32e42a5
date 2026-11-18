from dataclasses import dataclass
from columnlake.source.events import DataError, exact_int
from columnlake.source.schema import TypeSpec


@dataclass(frozen=True, order=True)
class GenerationKey:
    stream_id: str
    dictionary_id: int
    generation: int

    def to_obj(self):
        return [self.stream_id, self.dictionary_id, self.generation]

    @classmethod
    def from_obj(cls, raw):
        if (type(raw) is not list or len(raw) != 3 or type(raw[0]) is not str
                or not raw[0] or not exact_int(raw[1]) or not exact_int(raw[2])):
            raise DataError('invalid generation key')
        return cls(*raw)


@dataclass(frozen=True)
class EncodedIndex:
    index: int


@dataclass(frozen=True)
class DictionaryGeneration:
    key: GenerationKey
    value_type: TypeSpec
    values: tuple


def replace_generation(key, value_type, values):
    return DictionaryGeneration(key, value_type, tuple(values))


def append_generation(key, previous, values):
    if (key.stream_id != previous.key.stream_id
            or key.dictionary_id != previous.key.dictionary_id
            or key.generation != previous.key.generation + 1):
        raise DataError('invalid append generation')
    return DictionaryGeneration(key, previous.value_type, previous.values + tuple(values))
