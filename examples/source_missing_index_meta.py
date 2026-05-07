"""Example custom rule: External source index tracking.

Demonstrates optional structured metadata kwargs (rationale, remediation,
examples) on the @rule decorator. These are not required for custom rules
but populate --list-rules output when provided.
"""

from __future__ import annotations

from dbt_lint.extend import Resource, RuleContext, Violation, rule


@rule(
    id="custom/source-missing-index-meta",
    description="External source table missing indexes_reviewed meta.",
    rationale=(
        "External systems don't always create indexes on new synced tables. Unindexed "
        "source tables may impact performance in downstream views "
        "and all models that depend on them."
        "\n\n"
        "Convention: FT source tables (name starts with ft_) should declare "
        "meta.indexes_reviewed: true in source YAML once indexes are confirmed."
    ),
    remediation=(
        "Confirm database indexes exist for the source table, then add "
        "indexes_reviewed: true to the table's meta in source YAML."
    ),
)
def source_missing_index_meta(
    resource: Resource, context: RuleContext
) -> Violation | None:
    if resource.resource_type != "source":
        return None

    # Scope to external sources (ft_ naming convention).
    if not resource.resource_name.startswith("ft_"):
        return None

    if resource.meta.get("indexes_reviewed"):
        return None

    return context.violation(
        resource,
        f"{resource.resource_name}: External source missing"
        " meta.indexes_reviewed; confirm database indexes exist"
        " and add indexes_reviewed: true to source YAML",
    )
