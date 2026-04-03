"""Layer boundary rules: source/staging/downstream dependency constraints."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import direct_edges, group_by, resolve_name, resources_by_id, rule


@rule(
    id="modeling/direct-join-to-source",
    description="Models referencing both source and model parents.",
    rationale=(
        "Models should not join source and model parents in the same query."
        "\n\n"
        "Mixing raw source data with transformed model data in a single "
        "model bypasses the staging layer's type casting and renaming. "
        "Maintain a 1:1 relationship between sources and staging models; "
        "no other model should read directly from a source."
    ),
    remediation=(
        "Create a staging model for the source if one is missing. "
        "Replace the source() call with a ref() to the staging model."
    ),
    examples=(
        "Violation: fct_orders refs source.raw.orders AND ref('dim_customers')",
        "Pass: fct_orders refs stg_orders AND ref('dim_customers')",
    ),
)
def direct_join_to_source(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    model_children = [e for e in edges if e.child_resource_type == "model"]
    by_child = group_by(model_children, key=lambda e: e.child)

    violations = []
    by_id = resources_by_id(resources)
    for child_id, parents in by_child.items():
        parent_types = {e.parent_resource_type for e in parents}
        if "source" in parent_types and "model" in parent_types:
            child = by_id.get(child_id)
            violations.append(
                Violation(
                    rule_id="modeling/direct-join-to-source",
                    resource_id=child_id,
                    resource_name=resolve_name(by_id, child_id),
                    message=(
                        f"{child_id}: joins both source and model parents directly"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/downstream-depends-on-source",
    description="Intermediate/marts models depending directly on sources.",
    rationale=(
        "Intermediate and marts models should not depend directly on sources."
        "\n\n"
        "Only staging models should reference sources. Downstream models "
        "should consume data through the staging layer, which provides a "
        "stable interface for type casting, renaming, and source isolation."
    ),
    remediation=(
        "Add a staging model as an abstraction layer between the "
        "raw data and the downstream model. Replace source() with "
        "ref() to the new staging model."
    ),
)
def downstream_depends_on_source(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    violations = []
    by_id = resources_by_id(resources)

    for edge in edges:
        if (
            edge.parent_resource_type == "source"
            and edge.child_resource_type == "model"
            and edge.child_model_type in ("intermediate", "marts")
        ):
            child = by_id.get(edge.child)
            violations.append(
                Violation(
                    rule_id="modeling/downstream-depends-on-source",
                    resource_id=edge.child,
                    resource_name=resolve_name(by_id, edge.child),
                    message=(
                        f"{edge.child}: {edge.child_model_type} model"
                        " depends directly on source"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/staging-depends-on-staging",
    description="Staging models depending on other staging models.",
    rationale=(
        "Staging models should not depend on other staging models."
        "\n\n"
        "Each staging model should map 1:1 to a source, performing only "
        "renaming, casting, and basic cleanup. When staging models reference "
        "each other, it creates hidden coupling between source pipelines "
        "and makes the staging layer harder to reason about."
    ),
    remediation=(
        "Change the dependent model's type to intermediate, or "
        "update its lineage to reference source() directly."
    ),
)
def staging_depends_on_staging(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    violations = []
    by_id = resources_by_id(resources)

    for edge in edges:
        if edge.parent_model_type == "staging" and edge.child_model_type == "staging":
            child = by_id.get(edge.child)
            violations.append(
                Violation(
                    rule_id="modeling/staging-depends-on-staging",
                    resource_id=edge.child,
                    resource_name=resolve_name(by_id, edge.child),
                    message=(
                        f"{edge.child}: staging model depends"
                        f" on staging model {edge.parent}"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/staging-depends-on-downstream",
    description="Staging models depending on intermediate/marts.",
    rationale=(
        "Staging models should not depend on intermediate or marts models."
        "\n\n"
        "Staging is the lowest transformation layer; it should only read "
        "from sources. A dependency on downstream models creates a cycle "
        "in the logical layer hierarchy, even if the DAG itself is acyclic."
    ),
    remediation=(
        "Rename the model with the appropriate prefix for its actual "
        "layer (e.g., int_), or change its lineage to reference "
        "source() instead."
    ),
)
def staging_depends_on_downstream(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    violations = []
    by_id = resources_by_id(resources)

    for edge in edges:
        if edge.child_model_type == "staging" and edge.parent_model_type in (
            "intermediate",
            "marts",
        ):
            child = by_id.get(edge.child)
            violations.append(
                Violation(
                    rule_id="modeling/staging-depends-on-downstream",
                    resource_id=edge.child,
                    resource_name=resolve_name(by_id, edge.child),
                    message=(
                        f"{edge.child}: staging model depends on"
                        f" {edge.parent_model_type} model {edge.parent}"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/root-models",
    description="Models with zero parents (no dependencies).",
    rationale=(
        "Models should have at least one parent."
        "\n\n"
        "A model with no parents is either reading from a hard-coded "
        "reference (bypassing source declarations) or is an orphaned "
        "artifact. Both cases indicate missing lineage in the DAG."
    ),
    remediation=(
        "Map table references in FROM clauses to ref() or source(). "
        "Declare new sources in YAML if needed."
    ),
    exceptions=(
        "Self-contained utility models like dim_calendar generated "
        "by dbt_utils.date_spine() that have no upstream data."
    ),
)
def root_models(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    models_with_parents = {e.child for e in edges if e.child_resource_type == "model"}

    violations = []
    for r in resources:
        if r.resource_type == "model" and r.resource_id not in models_with_parents:
            violations.append(
                Violation(
                    rule_id="modeling/root-models",
                    resource_id=r.resource_id,
                    resource_name=r.resource_name,
                    message=f"{r.resource_name}: model has no parents",
                    severity=config.severity,
                    file_path=r.file_path,
                )
            )
    return violations
