# Custom rules

Write project-specific rules in Python using the same `@rule` decorator as built-in rules. Custom rules are loaded dynamically via the `source:` directive in config.

## Minimal example

```python
from dbt_lint.extend import Resource, RuleConfig, Violation, rule


@rule(
    id="custom/no-hardcoded-schema",
    description="Model sets a hardcoded schema.",
)
def no_hardcoded_schema(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type == "model" and resource.config.get("schema"):
        return Violation.from_resource(
            resource, f"{resource.resource_name}: hardcoded schema"
        )
    return None
```

That's a complete rule. The `@rule` decorator requires `id` and `description`. The function receives a `Resource` and returns a `Violation` or `None`.

## Adding metadata

Optional decorator fields populate `--list-rules` output:

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

- Per-resource: `(resource: Resource, config: RuleConfig) -> Violation | None`
- Aggregate: `(resources: list[Resource], relationships: list[Relationship], config: RuleConfig) -> list[Violation]`

Use aggregate when the rule needs to compare across resources or traverse the DAG. The decorator validates the signature at import time.

## Registering in config

Add a `source:` key to the rule entry in `dbt_lint.yml`. The path is relative to the config file.

```yaml
rules:
  custom/no-hardcoded-schema:
    source: custom_rules/no_hardcoded_schema.py
    severity: warn
```

Custom rule IDs must not collide with built-in rule IDs (the loader raises an error on collision). Multiple rules can live in the same file; the loader matches by `id`.

## Public API (`dbt_lint.extend`)

| Export | Purpose |
| --- | --- |
| `Resource` | Frozen dataclass with 25 fields (resource_id, resource_name, resource_type, raw_code, meta, config, tags, columns, ...) |
| `Relationship` | Dependency edge between resources (parent, child, distance, ...) |
| `Violation` | Rule violation. Use `Violation.from_resource(resource, message)` to create. |
| `RuleConfig` | Per-rule config (enabled, severity, params dict) |
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
