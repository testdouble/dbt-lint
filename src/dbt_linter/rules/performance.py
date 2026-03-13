"""Performance rules: chained views, exposure materializations."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import direct_edges, rule


@rule(
    id="performance/chained-views",
    description="View chains exceeding depth threshold.",
)
def chained_views(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    threshold = config.params.get("chained_views_threshold", 5)
    resources_by_id = {r.resource_id: r for r in resources}
    violations = []
    seen = set()

    for rel in relationships:
        if (
            rel.is_dependent_on_chain_of_views
            and rel.distance > threshold
            and rel.child not in seen
        ):
            seen.add(rel.child)
            child = resources_by_id.get(rel.child)
            violations.append(
                Violation(
                    rule_id="performance/chained-views",
                    resource_id=rel.child,
                    resource_name=(child.resource_name if child else rel.child),
                    message=(
                        f"{rel.child}: view chain depth {rel.distance}"
                        f" exceeds threshold {threshold}"
                    ),
                    severity=config.severity,
                    file_path=child.file_path if child else "",
                )
            )
    return violations


@rule(
    id="performance/exposure-parent-materializations",
    description="Exposures with view/ephemeral/source parents.",
)
def exposure_parent_materializations(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    resources_by_id = {r.resource_id: r for r in resources}
    edges = direct_edges(relationships)
    exposure_edges = [e for e in edges if e.child_resource_type == "exposure"]

    violations = []
    for edge in exposure_edges:
        parent = resources_by_id.get(edge.parent)
        if not parent:
            continue
        bad_mats = ("view", "ephemeral")
        is_bad = parent.resource_type == "source" or parent.materialization in bad_mats
        if is_bad:
            exposure = resources_by_id.get(edge.child)
            violations.append(
                Violation(
                    rule_id="performance/exposure-parent-materializations",
                    resource_id=edge.child,
                    resource_name=(exposure.resource_name if exposure else edge.child),
                    message=(
                        f"{edge.child}: exposure depends on"
                        f" {parent.resource_name}"
                        f" ({parent.materialization or parent.resource_type})"
                    ),
                    severity=config.severity,
                    file_path=(exposure.file_path if exposure else ""),
                )
            )
    return violations
