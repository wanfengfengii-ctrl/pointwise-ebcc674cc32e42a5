from dataclasses import dataclass
from copy import deepcopy
from columnlake.source.schema import parse_schema, path_tokens, resolve_type


@dataclass(frozen=True)
class ProjectionPlan:
    schema: object
    tokens: tuple
    result_type: object


def compile_projection(schema, path):
    schema = parse_schema(schema)
    tokens = path_tokens(path)
    return ProjectionPlan(schema, tokens, resolve_type(schema, tokens))


def project_rows(plan, rows):
    def descend(t, value, remaining):
        if value is None:
            return None
        if not remaining:
            return deepcopy(value)
        if t.kind == 'dict':
            return descend(t.item, value, remaining)
        if t.kind == 'list':
            return [descend(t.item, child, remaining) for child in value]
        name = remaining[0]
        child = dict(t.fields)[name]
        return descend(child, value[name], remaining[1:])

    return [descend(plan.schema.root, row, plan.tokens) for row in rows]
