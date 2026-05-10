# Custom rules

Write project-specific rules in Python using the same `@rule` decorator as built-in rules. Custom rules are loaded dynamically via the `source:` directive in config.

## Minimal example

```python
from dbt_lint.extend import Resource, RuleContext, Violation, rule


@rule(
    id="custom/no-hardcoded-schema",
    description="Model sets a hardcoded schema.",
)
def no_hardcoded_schema(resource: Resource, context: RuleContext) -> Violation | None:
    if resource.resource_type == "model" and resource.config.get("schema"):
        return context.violation(
            resource, f"{resource.resource_name}: hardcoded schema"
        )
    return None
```

That's a complete rule. The `@rule` decorator requires `id` and `description`. The function receives a `Resource` plus a `RuleContext` and returns a `Violation` or `None`.

## Building violations

`RuleContext` is the single surface rules use to construct violations and read configurable values:

- `context.violation(resource, message)` — the common path. Builds a fully-formed `Violation` keyed off the supplied `Resource`.
- `context.violation_for(*, resource_id, resource_name, message, file_path="", patch_path="")` — keyword-only escape hatch for aggregate rules that emit violations keyed by a synthetic identifier (e.g., a `model_type` bucket) or for edge-walking rules whose lookup may miss a `Resource`.
- `context.params["threshold"]` — read rule-relevant configuration values. Single-step access; the engine populates `params` from the `params:` block on the rule's config entry.

Rules never construct `Violation` directly. The engine populates `rule_id` and `severity` on the context before the call, so `violation()` / `violation_for()` always return fully-formed violations.

## Adding metadata

Optional decorator fields populate `dbt-lint rule --all` output:

```python
@rule(
    id="custom/no-hardcoded-schema",
    description="Model sets a hardcoded schema.",
    rationale="Hardcoded schemas break across environments.",
    remediation="Use generate_schema_name macro instead.",
)
```

## Function signatures

Two signatures are supported:

- Per-resource: `(resource: Resource, context: RuleContext) -> Violation | None`
- Aggregate: `(resources: list[Resource], relationships: list[Relationship], context: RuleContext) -> list[Violation]`

Use aggregate when the rule needs to compare across resources or traverse the DAG. The decorator validates the signature at import time.

## Registering in config

Add a `source:` key to the rule entry in `dbt-lint.yml`. The path is relative to the config file.

```yaml
rules:
  custom/no-hardcoded-schema:
    source: custom_rules/no_hardcoded_schema.py
    severity: warn
```

Custom rule IDs must not collide with built-in rule IDs (the Registry raises an error on collision). Multiple rules can live in the same file; the Registry matches by `id`.

## Public API (`dbt_lint.extend`)

| Export | Purpose |
| --- | --- |
| `Resource` | Frozen dataclass with 25 fields (resource_id, resource_name, resource_type, raw_code, meta, config, tags, columns, ...) |
| `Relationship` | Dependency edge between resources (parent, child, distance, ...) |
| `Violation` | Rule violation dataclass (rule_id, resource_id, resource_name, message, severity, file_path, patch_path) |
| `RuleContext` | Per-rule context: `params: dict[str, Any]`, `violation(resource, message)`, `violation_for(*, resource_id, resource_name, message, ...)` |
| `ColumnInfo` | Column metadata (name, data_type, is_described) |
| `rule` | Decorator: `@rule(id, description, *, rationale, remediation, exceptions, examples)` |
| `direct_edges` | Filter relationships to distance=1 |
| `filter_by_model_type` | Filter resources by model type |
| `group_by` | Group items by key function |

## Examples

See `examples/` for 5 working custom rules with tests:

| Example | Technique |
| --- | --- |
| `avoid_select_distinct` | `raw_code` regex matching |
| `source_missing_index_meta` | `meta` dict inspection |
| `staging_table_missing_indexes` | `config` dict access |
| `model_requires_scheduling_tag` | `tags` validation |
| `staging_no_cte_wrapping` | `raw_code` + `materialization` scoping |
