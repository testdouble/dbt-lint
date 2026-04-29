"""Graph structure rules: fanout, joins, mart dependencies, duplicate concepts."""

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

MIN_DUPLICATE_MART_NAMES = 2
MIN_PARENTS_FOR_DEPENDENCY_TRIAD = 2


@rule(
    id="modeling/source-fanout",
    description="Sources that are direct parents of multiple models.",
    rationale=(
        "Sources should have exactly one direct child: a staging model."
        "\n\n"
        "When multiple models read from the same source, schema changes in "
        "the upstream system require coordinated updates across all "
        "consumers. A single staging model acts as a contract boundary, "
        "isolating downstream models from source volatility."
    ),
    remediation=(
        "Create a single staging model per source. Refactor "
        "downstream models to reference the staging model instead "
        "of the source directly."
    ),
    exceptions=(
        "NoSQL or heavily nested data sources that need multiple "
        "base models to stage different aspects of the data."
    ),
    examples=(
        "Violation: source.raw.users -> [stg_users, dim_profiles]",
        "Pass: source.raw.users -> stg_users -> [dim_users, fct_orders]",
    ),
)
def source_fanout(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    edges = direct_edges(relationships)
    source_edges = [e for e in edges if e.parent_resource_type == "source"]
    by_parent = group_by(source_edges, key=lambda e: e.parent)

    violations = []
    by_id = resources_by_id(resources)
    for parent_id, children in by_parent.items():
        unique_children = {e.child for e in children}
        if len(unique_children) > 1:
            parent = by_id.get(parent_id)
            violations.append(
                context.violation_for(
                    resource_id=parent_id,
                    resource_name=resolve_name(by_id, parent_id),
                    message=f"{parent_id}: fans out to {len(unique_children)} models",
                    file_path=parent.file_path if parent else "",
                )
            )
    return violations


@rule(
    id="modeling/model-fanout",
    description="Models with too many direct dependents.",
    rationale=(
        "Models should not have too many direct dependents."
        "\n\n"
        "High fanout suggests a model is doing too much or that an "
        "intermediate model is needed to encapsulate shared logic. "
        "Refactoring reduces the blast radius of changes to the parent."
        "\n\n"
        "Configurable via models_fanout_threshold (default: 3)."
    ),
    remediation=(
        "Define an end point for your dbt project. Move "
        "reporting-specific logic to the BI layer or consolidate "
        "into fewer downstream models."
    ),
    exceptions=(
        "BI tools like Looker that join marts directly, or Tableau "
        "workbooks that benefit from pre-joined tables."
    ),
)
def model_fanout(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    threshold = context.params.get("models_fanout_threshold", 3)
    edges = direct_edges(relationships)
    model_edges = [e for e in edges if e.parent_resource_type == "model"]
    by_parent = group_by(model_edges, key=lambda e: e.parent)

    violations = []
    by_id = resources_by_id(resources)
    for parent_id, children in by_parent.items():
        unique_children = {e.child for e in children}
        if len(unique_children) >= threshold:
            parent = by_id.get(parent_id)
            violations.append(
                context.violation_for(
                    resource_id=parent_id,
                    resource_name=resolve_name(by_id, parent_id),
                    message=(
                        f"{parent_id}: fans out to"
                        f" {len(unique_children)} dependents"
                        f" (threshold: {threshold})"
                    ),
                    file_path=parent.file_path if parent else "",
                )
            )
    return violations


@rule(
    id="modeling/too-many-joins",
    description="Models with too many direct parents.",
    rationale=(
        "Models should not join too many direct parents."
        "\n\n"
        "A model with many parents is likely overly complex and doing too "
        "much in a single transformation. Breaking it into intermediate "
        "models improves readability and testability."
        "\n\n"
        "Configurable via too_many_joins_threshold (default: 7)."
    ),
    remediation=(
        "Break into intermediate models of 4-6 entities each, then "
        "join the intermediates in the final model."
    ),
)
def too_many_joins(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    threshold = context.params.get("too_many_joins_threshold", 7)
    edges = direct_edges(relationships)
    model_children = [e for e in edges if e.child_resource_type == "model"]
    by_child = group_by(model_children, key=lambda e: e.child)

    violations = []
    by_id = resources_by_id(resources)
    for child_id, parents in by_child.items():
        if len(parents) >= threshold:
            child = by_id.get(child_id)
            violations.append(
                context.violation_for(
                    resource_id=child_id,
                    resource_name=resolve_name(by_id, child_id),
                    message=(
                        f"{child_id}: {len(parents)} parents (threshold: {threshold})"
                    ),
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/staging-model-too-many-parents",
    description="Staging models with more than one parent (no joins in staging).",
    rationale=(
        "Staging models should have at most one parent (no joins)."
        "\n\n"
        "A staging model should be a 1:1 mapping of a single source. "
        "Multiple parents indicate joins happening in the staging layer, "
        "which should be deferred to intermediate or marts models."
        "\n\n"
        "Configurable via staging_max_parents (default: 1)."
    ),
    remediation=(
        "Move join logic to an intermediate model. The staging "
        "model should select from a single source only."
    ),
    exceptions=(
        "Base models that join a separate delete table to mark or "
        "filter deleted records before the staging model."
    ),
)
def staging_model_too_many_parents(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    threshold = context.params.get("staging_max_parents", 1)
    edges = direct_edges(relationships)
    staging_edges = [e for e in edges if e.child_model_type == "staging"]
    by_child = group_by(staging_edges, key=lambda e: e.child)

    violations = []
    by_id = resources_by_id(resources)
    for child_id, parents in by_child.items():
        unique_parents = {e.parent for e in parents}
        if len(unique_parents) > threshold:
            child = by_id.get(child_id)
            violations.append(
                context.violation_for(
                    resource_id=child_id,
                    resource_name=resolve_name(by_id, child_id),
                    message=(
                        f"{child_id}: staging model has"
                        f" {len(unique_parents)} parents"
                        f" (max: {threshold})"
                    ),
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/intermediate-fanout",
    description="Intermediate models with too many direct dependents.",
    rationale=(
        "Intermediate models should have limited direct dependents."
        "\n\n"
        "Intermediates encapsulate shared logic for a specific downstream "
        "consumer. High fanout suggests the intermediate is really a mart "
        "or should be promoted to a more visible layer."
        "\n\n"
        "Configurable via intermediate_fanout_threshold (default: 1)."
    ),
    remediation=(
        "Promote the intermediate to a mart if it serves multiple "
        "consumers, or restructure so each intermediate feeds a "
        "single downstream model."
    ),
)
def intermediate_fanout(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    threshold = context.params.get("intermediate_fanout_threshold", 1)
    edges = direct_edges(relationships)
    inter_edges = [
        e
        for e in edges
        if e.parent_model_type == "intermediate" and e.parent_resource_type == "model"
    ]
    by_parent = group_by(inter_edges, key=lambda e: e.parent)

    violations = []
    by_id = resources_by_id(resources)
    for parent_id, children in by_parent.items():
        unique_children = {e.child for e in children}
        if len(unique_children) > threshold:
            parent = by_id.get(parent_id)
            violations.append(
                context.violation_for(
                    resource_id=parent_id,
                    resource_name=resolve_name(by_id, parent_id),
                    message=(
                        f"{parent_id}: intermediate model fans out to"
                        f" {len(unique_children)} dependents"
                        f" (max: {threshold})"
                    ),
                    file_path=parent.file_path if parent else "",
                )
            )
    return violations


@rule(
    id="modeling/mart-depends-on-mart",
    description="Mart models depending on other mart models.",
    rationale=(
        "Building one mart on another mart requires careful consideration."
        "\n\n"
        "Mart-to-mart dependencies create tight coupling between "
        "consumer-facing models. Changes to a parent mart can break "
        "downstream marts without intermediate layers to absorb the impact. "
        "Consider whether the child should depend on an intermediate instead."
    ),
    remediation=(
        "Extract shared logic into an intermediate model. Have both "
        "marts depend on the intermediate rather than on each other."
    ),
    exceptions=(
        "Intentionally layered marts where one builds directly on "
        "another (e.g., a reporting mart that aggregates a detail mart)."
    ),
)
def mart_depends_on_mart(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    edges = direct_edges(relationships)
    by_id = resources_by_id(resources)

    violations = []
    for edge in edges:
        if edge.parent_model_type == "marts" and edge.child_model_type == "marts":
            child = by_id.get(edge.child)
            violations.append(
                context.violation_for(
                    resource_id=edge.child,
                    resource_name=resolve_name(by_id, edge.child),
                    message=(
                        f"{resolve_name(by_id, edge.child)}:"
                        f" mart depends on mart"
                        f" {resolve_name(by_id, edge.parent)}"
                    ),
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/duplicate-mart-concepts",
    description="Same entity modeled in multiple mart subdirectories.",
    rationale=(
        "The same entity should not appear as a mart in multiple directories."
        "\n\n"
        "Duplicate mart names across subdirectories (e.g., finance/dim_users "
        "and marketing/dim_users) create ambiguity about which is the "
        "canonical model. One source of truth per entity."
    ),
    remediation=(
        "Consolidate into a single shared mart, or rename to "
        "clarify the distinct concepts (e.g., tax_revenue vs "
        "revenue rather than finance_orders vs marketing_orders)."
    ),
)
def duplicate_mart_concepts(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    marts = [
        r for r in resources if r.resource_type == "model" and r.model_type == "marts"
    ]
    by_name = group_by(marts, key=lambda r: r.resource_name)

    violations = []
    for name, group in by_name.items():
        if len(group) < MIN_DUPLICATE_MART_NAMES:
            continue
        dirs = {r.file_path.rsplit("/", 1)[0] for r in group}
        if len(dirs) > 1:
            violations.append(
                context.violation(
                    group[0],
                    f"{name}: same mart entity appears in"
                    f" {len(dirs)} directories"
                    f" ({', '.join(sorted(dirs))})",
                )
            )
    return violations


@rule(
    id="modeling/rejoining-upstream-concepts",
    description="Models that rejoin a previously consumed concept.",
    rationale=(
        "Models should not rejoin a previously consumed upstream concept."
        "\n\n"
        'A "rejoin" occurs when model C depends on both A and B, where B '
        "already depends on A (the A->B->C, A->C triad). This often means "
        "C could get what it needs from B alone, and the direct A->C edge "
        "creates redundant coupling that adds complexity without enabling "
        "parallelism."
    ),
    remediation=(
        "Fold the intermediate model's SQL into a CTE within the "
        "downstream model, or remove the direct dependency on the "
        "ancestor if the intermediate already provides what's needed."
    ),
    exceptions=(
        "When using dbt_utils functions (e.g., star, "
        "get_column_values) that require a relation as input and "
        "the shape differs from the intermediate parent."
    ),
    examples=(
        "Violation: fct_orders refs stg_users AND dim_users "
        "(dim_users already refs stg_users)",
        "Pass: fct_orders refs only dim_users",
    ),
)
def rejoining_upstream_concepts(
    resources: list[Resource],
    relationships: list[Relationship],
    context: RuleContext,
) -> list[Violation]:
    edges = direct_edges(relationships)
    by_child = group_by(edges, key=lambda e: e.child)

    violations = []
    by_id = resources_by_id(resources)

    for child_id, child_edges in by_child.items():
        parent_ids = {e.parent for e in child_edges}
        if len(parent_ids) < MIN_PARENTS_FOR_DEPENDENCY_TRIAD:
            continue

        # For each parent of child, check if that parent also has
        # a parent that is another parent of child (the triad)
        parent_parents = {}
        for pid in parent_ids:
            if pid in by_child:
                parent_parents[pid] = {e.parent for e in by_child[pid]}

        for mid_id, mid_parents in parent_parents.items():
            shared = parent_ids & mid_parents
            for ancestor_id in shared:
                child = by_id.get(child_id)
                violations.append(
                    context.violation_for(
                        resource_id=child_id,
                        resource_name=resolve_name(by_id, child_id),
                        message=(
                            f"{child_id}: rejoins {ancestor_id}"
                            f" (already consumed via {mid_id})"
                        ),
                        file_path=child.file_path if child else "",
                    )
                )
    return violations
