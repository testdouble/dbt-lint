"""Dependency hygiene rules: hard-coded refs, duplicate/unused sources, multi-source joins."""

from __future__ import annotations

from dbt_lint.models import Relationship, Resource, Violation
from dbt_lint.rules import (
    RuleContext,
    direct_edges,
    group_by,
    resolve_name,
    resources_by_id,
    rule,
)


@rule(
    id="modeling/hard-coded-references",
    description="Models with hard-coded table references in SQL.",
    rationale=(
        "Models should not contain hard-coded table references in SQL."
        "\n\n"
        "Hard-coded references (e.g., `schema.table` instead of "
        "`{{ ref('model') }}` or `{{ source('src', 'table') }}`) bypass "
        "dbt's dependency graph. This breaks lineage tracking, prevents "
        "environment-aware schema resolution, and makes refactoring fragile."
    ),
    remediation=(
        "Replace hard-coded references with ref() or source(). "
        "Create source definitions in YAML if the table doesn't "
        "have one yet."
    ),
)
def hard_coded_references(resource: Resource, context: RuleContext) -> Violation | None:
    if resource.resource_type == "model" and resource.hard_coded_references:
        return context.violation(
            resource,
            f"{resource.resource_name}: contains hard-coded table references",
        )
    return None


@rule(
    id="modeling/duplicate-sources",
    description="Multiple source entries pointing to the same table.",
    rationale=(
        "Each database table should be declared as a source at most once."
        "\n\n"
        "Duplicate source entries for the same database.schema.table create "
        "ambiguity about which source definition is authoritative and can "
        "lead to inconsistent freshness checks or descriptions."
    ),
    remediation=(
        "Combine duplicate source nodes into a single definition. "
        "Update all source() references to use the canonical entry."
    ),
)
def duplicate_sources(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    sources = [r for r in resources if r.resource_type == "source"]
    by_target = group_by(
        sources,
        key=lambda s: (s.database, s.schema_name, s.resource_name),
    )

    violations = []
    for table_key, source_group in by_target.items():
        if len(source_group) > 1:
            violations.append(
                context.violation(
                    source_group[0],
                    f"{table_key[0]}.{table_key[1]}.{table_key[2]}:"
                    f" {len(source_group)} duplicate source entries",
                )
            )
    return violations


@rule(
    id="modeling/unused-sources",
    description="Sources with no downstream consumers.",
    rationale=(
        "Every declared source should have at least one downstream consumer."
        "\n\n"
        "An unused source definition adds clutter to the YAML and dbt docs "
        "without contributing to the project. It may indicate a removed "
        "pipeline that wasn't fully cleaned up."
    ),
    remediation=("Remove the unused table entry from the source YAML definition."),
)
def unused_sources(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    edges = direct_edges(relationships)
    sources_with_children = {
        e.parent for e in edges if e.parent_resource_type == "source"
    }

    violations = []
    for r in resources:
        if r.resource_type == "source" and r.resource_id not in sources_with_children:
            violations.append(
                context.violation(r, f"{r.resource_name}: source has no consumers")
            )
    return violations


@rule(
    id="modeling/multiple-sources-joined",
    description="Models joining more than one source directly.",
    rationale=(
        "Models should not join multiple sources directly."
        "\n\n"
        "Each staging model should wrap exactly one source. When a model "
        "joins multiple sources, it combines raw data from different "
        "upstream systems in a single transformation, making it harder to "
        "isolate source-specific changes."
    ),
    remediation=(
        "Split into individual staging models per source. Combine "
        "them in an intermediate model. Or use base__ models as "
        "transitional steps."
    ),
    exceptions=(
        "Identical sources across systems that are only used "
        "collectively (union pattern, e.g., multiple Shopify stores)."
    ),
    examples=(
        "Violation: stg_orders refs source.stripe.charges AND source.shopify.orders",
        "Pass: stg_stripe_charges refs only source.stripe.charges",
    ),
)
def multiple_sources_joined(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    edges = direct_edges(relationships)
    source_edges = [
        e
        for e in edges
        if e.parent_resource_type == "source" and e.child_resource_type == "model"
    ]
    by_child = group_by(source_edges, key=lambda e: e.child)

    violations = []
    by_id = resources_by_id(resources)
    for child_id, parents in by_child.items():
        if len(parents) > 1:
            child = by_id.get(child_id)
            violations.append(
                context.violation_for(
                    resource_id=child_id,
                    resource_name=resolve_name(by_id, child_id),
                    message=f"{child_id}: joins {len(parents)} sources directly",
                    file_path=child.file_path if child else "",
                )
            )
    return violations
