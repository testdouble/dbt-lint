"""Example custom rule: staging table index configuration.

Demonstrates optional structured metadata kwargs (rationale, remediation)
on the @rule decorator. These populate --list-rules output when provided.
"""

from __future__ import annotations

from dbt_linter.extend import Resource, RuleConfig, Violation, rule

_INDEXED_MATERIALIZATIONS = {"table", "incremental"}


@rule(
    id="custom/staging-table-missing-indexes",
    description="Table/incremental staging model missing indexes config.",
    rationale=(
        "When staging models are materialized as tables (deviating from the "
        "dbt default of views), they should declare indexes in their config "
        "for query performance. dbt-postgres supports config.indexes natively."
    ),
    remediation=(
        "Add an indexes key to the model's config in dbt_project.yml or "
        "the model's config block."
    ),
)
def staging_table_missing_indexes(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if resource.resource_type != "model":
        return None
    if resource.model_type != "staging":
        return None
    if resource.materialization not in _INDEXED_MATERIALIZATIONS:
        return None

    indexes = resource.config.get("indexes")
    if indexes:
        return None

    return Violation.from_resource(
        resource,
        f"{resource.resource_name}: staging model materialized as"
        f" {resource.materialization} without indexes config;"
        " add indexes for query performance",
    )
