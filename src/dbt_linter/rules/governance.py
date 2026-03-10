"""Governance rules: public model contracts, exposure access."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import direct_edges, group_by, rule


@rule(
    id="governance/public-models-without-contract",
    description="Public models without contract enforcement.",
)
def public_models_without_contract(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if (
        resource.resource_type == "model"
        and resource.is_public
        and not resource.is_contract_enforced
    ):
        return Violation(
            rule_id="governance/public-models-without-contract",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(
                f"{resource.resource_name}: public model"
                " without contract enforcement"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="governance/undocumented-public-models",
    description="Public models missing description or column docs.",
)
def undocumented_public_models(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if resource.resource_type != "model" or not resource.is_public:
        return None
    issues = []
    if not resource.is_described:
        issues.append("missing description")
    if (
        resource.number_of_columns > 0
        and resource.number_of_documented_columns < resource.number_of_columns
    ):
        issues.append(
            f"{resource.number_of_documented_columns}/"
            f"{resource.number_of_columns} columns documented"
        )
    if issues:
        return Violation(
            rule_id="governance/undocumented-public-models",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(
                f"{resource.resource_name}: {', '.join(issues)}"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="governance/exposures-depend-on-private-models",
    description="Exposures with non-public model parents.",
)
def exposures_depend_on_private_models(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    resources_by_id = {r.resource_id: r for r in resources}
    edges = direct_edges(relationships)
    exposure_edges = [
        e for e in edges if e.child_resource_type == "exposure"
    ]

    violations = []
    by_exposure = group_by(exposure_edges, key=lambda e: e.child)
    for exposure_id, parents in by_exposure.items():
        exposure = resources_by_id.get(exposure_id)
        if not exposure:
            continue
        for edge in parents:
            parent = resources_by_id.get(edge.parent)
            if (
                parent
                and parent.resource_type == "model"
                and not parent.is_public
            ):
                violations.append(
                    Violation(
                        rule_id=(
                            "governance/"
                            "exposures-depend-on-private-models"
                        ),
                        resource_id=exposure_id,
                        resource_name=exposure.resource_name,
                        message=(
                            f"{exposure.resource_name}: depends on"
                            f" non-public model"
                            f" {parent.resource_name}"
                        ),
                        severity=config.severity,
                        file_path=exposure.file_path,
                    )
                )
    return violations
