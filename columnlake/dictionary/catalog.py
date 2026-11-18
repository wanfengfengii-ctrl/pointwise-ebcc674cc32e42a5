from columnlake.source.events import DataError, exact_int
from columnlake.source.schema import parse_type
from columnlake.dictionary.generations import (
    GenerationKey, DictionaryGeneration, replace_generation, append_generation,
)
from columnlake.dictionary.references import bind_references, dump_cell, load_cell, dependency_closure


class DictionaryCatalog:
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.current = {}
        self.history = {}

    def clone(self):
        other = DictionaryCatalog(self.stream_id)
        other.current = dict(self.current)
        other.history = dict(self.history)
        return other

    def lookup(self, dictionary_id):
        if dictionary_id not in self.current:
            raise DataError('missing dictionary')
        return self.history[self.current[dictionary_id]]

    def _apply(self, dictionary_id, value_type, values, delta):
        if not exact_int(dictionary_id):
            raise DataError('invalid dictionary number')
        value_type = parse_type(value_type)
        previous = self.lookup(dictionary_id) if dictionary_id in self.current else None
        if delta and previous is None:
            raise DataError('delta requires an existing dictionary')
        if previous is not None and previous.value_type != value_type:
            raise DataError('dictionary type is fixed')
        bound = tuple(bind_references(value_type, cell, self) for cell in values)
        dependency_closure(bound)
        number = 0 if previous is None else previous.key.generation + 1
        key = GenerationKey(self.stream_id, dictionary_id, number)
        generation = (append_generation(key, previous, bound) if delta
                      else replace_generation(key, value_type, bound))
        self.history[key] = generation
        self.current[dictionary_id] = key
        return generation

    def apply_replace(self, dictionary_id, value_type, values):
        return self._apply(dictionary_id, value_type, values, False)

    def apply_delta(self, dictionary_id, value_type, values):
        return self._apply(dictionary_id, value_type, values, True)

    def export_state(self):
        return {
            'stream_id': self.stream_id,
            'current': [key.to_obj() for _, key in sorted(self.current.items())],
            'history': [
                {'key': key.to_obj(), 'type': generation.value_type.to_obj(),
                 'values': [dump_cell(generation.value_type, cell) for cell in generation.values]}
                for key, generation in sorted(self.history.items())
            ],
        }

    @classmethod
    def from_state(cls, raw):
        if type(raw) is not dict or set(raw) != {'stream_id', 'current', 'history'}:
            raise DataError('invalid dictionary catalog')
        if type(raw['stream_id']) is not str or not raw['stream_id']:
            raise DataError('invalid catalog stream')
        result = cls(raw['stream_id'])
        entries = {}
        for entry in raw['history']:
            if type(entry) is not dict or set(entry) != {'key', 'type', 'values'}:
                raise DataError('invalid generation record')
            key = GenerationKey.from_obj(entry['key'])
            if key.stream_id != result.stream_id or key in entries:
                raise DataError('duplicate or foreign generation')
            if type(entry['values']) is not list:
                raise DataError('invalid generation values')
            entries[key] = entry
        active = set()

        def restore(key):
            if key in active or key not in entries:
                raise DataError('cyclic or missing stored generation')
            if key not in result.history:
                active.add(key)
                entry = entries[key]
                t = parse_type(entry['type'])
                cells = tuple(load_cell(t, cell, restore) for cell in entry['values'])
                active.remove(key)
                result.history[key] = DictionaryGeneration(key, t, cells)
            return result.history[key]

        for key in entries:
            restore(key)
        for raw_key in raw['current']:
            key = GenerationKey.from_obj(raw_key)
            if key.dictionary_id in result.current or key not in result.history:
                raise DataError('invalid current generation')
            result.current[key.dictionary_id] = key
        groups = {}
        for key, generation in result.history.items():
            groups.setdefault(key.dictionary_id, []).append(generation)
        if set(groups) != set(result.current):
            raise DataError('incomplete current dictionary directory')
        for dictionary_id, generations in groups.items():
            generations.sort(key=lambda g: g.key.generation)
            if [g.key.generation for g in generations] != list(range(len(generations))):
                raise DataError('noncontinuous dictionary history')
            if any(g.value_type != generations[0].value_type for g in generations):
                raise DataError('stored dictionary changed type')
            if result.current[dictionary_id] != generations[-1].key:
                raise DataError('current dictionary is not the latest generation')
        return result
