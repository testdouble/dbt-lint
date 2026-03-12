"""Flag Fivetran source tables missing indexes_reviewed meta key.

From PR #935 analysis: Fivetran doesn't create indexes on new synced tables.
Unindexed source tables may impact performance in downstream views
and all models that depend on them.

Convention: FT source tables (name starts with ft_) should declare
meta.indexes_reviewed: true in source YAML once indexes are confirmed.
"""

from __future__ import annotations

from dbt_linter.extend import Resource, RuleConfig, Violation, rule


@rule(
    id="custom/source-missing-index-meta",
    description="External source table missing indexes_reviewed meta.",
)
def source_missing_index_meta(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if resource.resource_type != "source":
        return None

    # Scope to external sources (ft_ naming convention).
    if not resource.resource_name.startswith("ft_"):
        return None

    if resource.meta.get("indexes_reviewed"):
        return None

    return Violation.from_resource(
        resource,
        f"{resource.resource_name}: External source missing"
        " meta.indexes_reviewed; confirm database indexes exist"
        " and add indexes_reviewed: true to source YAML",
    )
