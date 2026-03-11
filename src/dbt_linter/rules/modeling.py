"""Modeling rules: DAG structure, layer boundaries, dependency hygiene."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import direct_edges, group_by, rule


@rule(
    id="modeling/direct-join-to-source",
    description="Models referencing both source and model parents.",
)
def direct_join_to_source(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    model_children = [
        e for e in edges if e.child_resource_type == "model"
    ]
    by_child = group_by(model_children, key=lambda e: e.child)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}
    for child_id, parents in by_child.items():
        parent_types = {e.parent_resource_type for e in parents}
        if "source" in parent_types and "model" in parent_types:
            child = resources_by_id.get(child_id)
            violations.append(
                Violation(
                    rule_id="modeling/direct-join-to-source",
                    resource_id=child_id,
                    resource_name=(
                        child.resource_name if child else child_id
                    ),
                    message=(
                        f"{child_id}: joins both source"
                        " and model parents directly"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/downstream-depends-on-source",
    description="Intermediate/marts models depending directly on sources.",
)
def downstream_depends_on_source(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    violations = []
    resources_by_id = {r.resource_id: r for r in resources}

    for edge in edges:
        if (
            edge.parent_resource_type == "source"
            and edge.child_resource_type == "model"
            and edge.child_model_type in ("intermediate", "marts")
        ):
            child = resources_by_id.get(edge.child)
            violations.append(
                Violation(
                    rule_id="modeling/downstream-depends-on-source",
                    resource_id=edge.child,
                    resource_name=(
                        child.resource_name if child else edge.child
                    ),
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
)
def staging_depends_on_staging(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    violations = []
    resources_by_id = {r.resource_id: r for r in resources}

    for edge in edges:
        if (
            edge.parent_model_type == "staging"
            and edge.child_model_type == "staging"
        ):
            child = resources_by_id.get(edge.child)
            violations.append(
                Violation(
                    rule_id="modeling/staging-depends-on-staging",
                    resource_id=edge.child,
                    resource_name=(
                        child.resource_name if child else edge.child
                    ),
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
)
def staging_depends_on_downstream(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    violations = []
    resources_by_id = {r.resource_id: r for r in resources}

    for edge in edges:
        if (
            edge.child_model_type == "staging"
            and edge.parent_model_type in ("intermediate", "marts")
        ):
            child = resources_by_id.get(edge.child)
            violations.append(
                Violation(
                    rule_id="modeling/staging-depends-on-downstream",
                    resource_id=edge.child,
                    resource_name=(
                        child.resource_name if child else edge.child
                    ),
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
)
def root_models(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    models_with_parents = {
        e.child for e in edges if e.child_resource_type == "model"
    }

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


@rule(
    id="modeling/hard-coded-references",
    description="Models with hard-coded table references in SQL.",
)
def hard_coded_references(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if resource.resource_type == "model" and resource.hard_coded_references:
        return Violation(
            rule_id="modeling/hard-coded-references",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(
                f"{resource.resource_name}:"
                " contains hard-coded table references"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="modeling/duplicate-sources",
    description="Multiple source entries pointing to the same table.",
)
def duplicate_sources(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    sources = [r for r in resources if r.resource_type == "source"]
    by_target = group_by(
        sources,
        key=lambda s: (s.database, s.schema_name, s.resource_name),
    )

    violations = []
    for key, group in by_target.items():
        if len(group) > 1:
            ids = [s.resource_id for s in group]
            violations.append(
                Violation(
                    rule_id="modeling/duplicate-sources",
                    resource_id=ids[0],
                    resource_name=group[0].resource_name,
                    message=(
                        f"{key[0]}.{key[1]}.{key[2]}:"
                        f" {len(group)} duplicate source entries"
                    ),
                    severity=config.severity,
                    file_path=group[0].file_path,
                )
            )
    return violations


@rule(
    id="modeling/unused-sources",
    description="Sources with no downstream consumers.",
)
def unused_sources(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    sources_with_children = {
        e.parent for e in edges if e.parent_resource_type == "source"
    }

    violations = []
    for r in resources:
        if (
            r.resource_type == "source"
            and r.resource_id not in sources_with_children
        ):
            violations.append(
                Violation(
                    rule_id="modeling/unused-sources",
                    resource_id=r.resource_id,
                    resource_name=r.resource_name,
                    message=(
                        f"{r.resource_name}: source has no consumers"
                    ),
                    severity=config.severity,
                    file_path=r.file_path,
                )
            )
    return violations


@rule(
    id="modeling/multiple-sources-joined",
    description="Models joining more than one source directly.",
)
def multiple_sources_joined(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    source_edges = [
        e for e in edges
        if e.parent_resource_type == "source"
        and e.child_resource_type == "model"
    ]
    by_child = group_by(source_edges, key=lambda e: e.child)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}
    for child_id, parents in by_child.items():
        if len(parents) > 1:
            child = resources_by_id.get(child_id)
            violations.append(
                Violation(
                    rule_id="modeling/multiple-sources-joined",
                    resource_id=child_id,
                    resource_name=(
                        child.resource_name if child else child_id
                    ),
                    message=(
                        f"{child_id}: joins {len(parents)}"
                        " sources directly"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/source-fanout",
    description="Sources that are direct parents of multiple models.",
)
def source_fanout(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    edges = direct_edges(relationships)
    source_edges = [
        e for e in edges if e.parent_resource_type == "source"
    ]
    by_parent = group_by(source_edges, key=lambda e: e.parent)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}
    for parent_id, children in by_parent.items():
        unique_children = {e.child for e in children}
        if len(unique_children) > 1:
            parent = resources_by_id.get(parent_id)
            violations.append(
                Violation(
                    rule_id="modeling/source-fanout",
                    resource_id=parent_id,
                    resource_name=(
                        parent.resource_name if parent else parent_id
                    ),
                    message=(
                        f"{parent_id}: fans out to"
                        f" {len(unique_children)} models"
                    ),
                    severity=config.severity,
                    file_path=parent.file_path if parent else "",
                )
            )
    return violations


@rule(
    id="modeling/model-fanout",
    description="Models with too many direct dependents.",
)
def model_fanout(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    threshold = config.params.get("models_fanout_threshold", 3)
    edges = direct_edges(relationships)
    model_edges = [
        e for e in edges if e.parent_resource_type == "model"
    ]
    by_parent = group_by(model_edges, key=lambda e: e.parent)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}
    for parent_id, children in by_parent.items():
        unique_children = {e.child for e in children}
        if len(unique_children) >= threshold:
            parent = resources_by_id.get(parent_id)
            violations.append(
                Violation(
                    rule_id="modeling/model-fanout",
                    resource_id=parent_id,
                    resource_name=(
                        parent.resource_name if parent else parent_id
                    ),
                    message=(
                        f"{parent_id}: fans out to"
                        f" {len(unique_children)} dependents"
                        f" (threshold: {threshold})"
                    ),
                    severity=config.severity,
                    file_path=parent.file_path if parent else "",
                )
            )
    return violations


@rule(
    id="modeling/too-many-joins",
    description="Models with too many direct parents.",
)
def too_many_joins(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    threshold = config.params.get("too_many_joins_threshold", 7)
    edges = direct_edges(relationships)
    model_children = [
        e for e in edges if e.child_resource_type == "model"
    ]
    by_child = group_by(model_children, key=lambda e: e.child)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}
    for child_id, parents in by_child.items():
        if len(parents) >= threshold:
            child = resources_by_id.get(child_id)
            violations.append(
                Violation(
                    rule_id="modeling/too-many-joins",
                    resource_id=child_id,
                    resource_name=(
                        child.resource_name if child else child_id
                    ),
                    message=(
                        f"{child_id}: {len(parents)} parents"
                        f" (threshold: {threshold})"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/staging-model-too-many-parents",
    description="Staging models with more than one parent (no joins in staging).",
)
def staging_model_too_many_parents(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    threshold = config.params.get("staging_max_parents", 1)
    edges = direct_edges(relationships)
    staging_edges = [
        e for e in edges if e.child_model_type == "staging"
    ]
    by_child = group_by(staging_edges, key=lambda e: e.child)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}
    for child_id, parents in by_child.items():
        unique_parents = {e.parent for e in parents}
        if len(unique_parents) > threshold:
            child = resources_by_id.get(child_id)
            violations.append(
                Violation(
                    rule_id="modeling/staging-model-too-many-parents",
                    resource_id=child_id,
                    resource_name=(
                        child.resource_name if child else child_id
                    ),
                    message=(
                        f"{child_id}: staging model has"
                        f" {len(unique_parents)} parents"
                        f" (max: {threshold})"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="modeling/intermediate-fanout",
    description="Intermediate models with too many direct dependents.",
)
def intermediate_fanout(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    threshold = config.params.get("intermediate_fanout_threshold", 1)
    edges = direct_edges(relationships)
    inter_edges = [
        e
        for e in edges
        if e.parent_model_type == "intermediate"
        and e.parent_resource_type == "model"
    ]
    by_parent = group_by(inter_edges, key=lambda e: e.parent)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}
    for parent_id, children in by_parent.items():
        unique_children = {e.child for e in children}
        if len(unique_children) > threshold:
            parent = resources_by_id.get(parent_id)
            violations.append(
                Violation(
                    rule_id="modeling/intermediate-fanout",
                    resource_id=parent_id,
                    resource_name=(
                        parent.resource_name if parent else parent_id
                    ),
                    message=(
                        f"{parent_id}: intermediate model fans out to"
                        f" {len(unique_children)} dependents"
                        f" (max: {threshold})"
                    ),
                    severity=config.severity,
                    file_path=parent.file_path if parent else "",
                )
            )
    return violations


@rule(
    id="modeling/duplicate-mart-concepts",
    description="Same entity modeled in multiple mart subdirectories.",
)
def duplicate_mart_concepts(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    marts = [
        r for r in resources
        if r.resource_type == "model" and r.model_type == "marts"
    ]
    by_name = group_by(marts, key=lambda r: r.resource_name)

    violations = []
    for name, group in by_name.items():
        if len(group) < 2:
            continue
        dirs = {r.file_path.rsplit("/", 1)[0] for r in group}
        if len(dirs) > 1:
            violations.append(
                Violation(
                    rule_id="modeling/duplicate-mart-concepts",
                    resource_id=group[0].resource_id,
                    resource_name=name,
                    message=(
                        f"{name}: same mart entity appears in"
                        f" {len(dirs)} directories"
                        f" ({', '.join(sorted(dirs))})"
                    ),
                    severity=config.severity,
                    file_path=group[0].file_path,
                )
            )
    return violations


@rule(
    id="modeling/rejoining-upstream-concepts",
    description="Models that rejoin a previously consumed concept.",
)
def rejoining_upstream_concepts(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    """Triad pattern: A->B->C and A->C at d=1.

    A "rejoined" concept is when model C depends on both A and B,
    where B also depends on A. This means C could potentially get
    what it needs from B alone.
    """
    edges = direct_edges(relationships)
    by_child = group_by(edges, key=lambda e: e.child)

    violations = []
    resources_by_id = {r.resource_id: r for r in resources}

    for child_id, child_edges in by_child.items():
        parent_ids = {e.parent for e in child_edges}
        if len(parent_ids) < 2:
            continue

        # For each parent of child, check if that parent also has
        # a parent that is another parent of child (the triad)
        parent_parents = {}
        for pid in parent_ids:
            if pid in by_child:
                parent_parents[pid] = {
                    e.parent for e in by_child[pid]
                }

        for mid_id, mid_parents in parent_parents.items():
            shared = parent_ids & mid_parents
            for ancestor_id in shared:
                child = resources_by_id.get(child_id)
                violations.append(
                    Violation(
                        rule_id="modeling/rejoining-upstream-concepts",
                        resource_id=child_id,
                        resource_name=(
                            child.resource_name
                            if child
                            else child_id
                        ),
                        message=(
                            f"{child_id}: rejoins {ancestor_id}"
                            f" (already consumed via {mid_id})"
                        ),
                        severity=config.severity,
                        file_path=(
                            child.file_path if child else ""
                        ),
                    )
                )
    return violations
