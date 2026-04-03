"""Governance rules: public model contracts, exposure access."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import direct_edges, group_by, resources_by_id, rule


@rule(
    id="governance/public-models-without-contract",
    description="Public models without contract enforcement.",
    rationale=(
        "Public models should enforce a contract."
        "\n\n"
        "A dbt contract guarantees the column names, types, and constraints "
        "of a model's output. Without one, downstream consumers of a public "
        "model can break silently when the schema changes. Contracts make "
        "the public interface explicit and testable."
    ),
    remediation=(
        "Add config.contract.enforced: true and column entries with "
        "data_type for every column in the model's YAML properties."
    ),
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
                f"{resource.resource_name}: public model without contract enforcement"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="governance/undocumented-public-models",
    description="Public models missing description or column docs.",
    rationale=(
        "Public models should have complete documentation."
        "\n\n"
        "Public models are the external API of a dbt project. Both a model "
        "description and column descriptions are expected so that consumers "
        "can understand the data without reading the implementation. "
        "Stricter than undocumented-models: requires both model and column "
        "descriptions."
    ),
    remediation=(
        "Add a description at the model level and on every column in "
        "the model's YAML properties file."
    ),
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
            message=(f"{resource.resource_name}: {', '.join(issues)}"),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="governance/intermediate-public-access",
    description="Intermediate models with public access.",
    rationale=(
        "Intermediate models should not have public access."
        "\n\n"
        "Intermediates are internal building blocks, not exposed to end users. "
        "They should be materialized ephemerally or as views in a restricted "
        "schema. Public access on an intermediate suggests a misconfigured "
        "access level or a model that should be promoted to a mart."
    ),
    remediation=(
        "Remove the access: public setting from the intermediate model. "
        "If the model needs to be consumed externally, promote it to a "
        "mart with appropriate contracts and documentation."
    ),
)
def intermediate_public_access(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    if (
        resource.resource_type == "model"
        and resource.model_type == "intermediate"
        and resource.is_public
    ):
        return Violation.from_resource(
            resource,
            f"{resource.resource_name}: intermediate model has public access",
        )
    return None


@rule(
    id="governance/exposures-depend-on-private-models",
    description="Exposures with non-public model parents.",
    rationale=(
        "Exposures should only depend on public models."
        "\n\n"
        "Exposures represent consumer-facing outputs (dashboards, ML models). "
        "If an exposure depends on a private model, that model's schema can "
        "change without considering the exposure. Routing through public "
        "models with contracts protects the exposure from breaking changes."
    ),
    remediation=(
        "Set access: public on models that exposures depend on, and "
        "add contracts and documentation to those models."
    ),
    examples=(
        "Violation: exposure.weekly_report -> int_orders (protected)",
        "Pass: exposure.weekly_report -> fct_orders (public)",
    ),
)
def exposures_depend_on_private_models(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    by_id = resources_by_id(resources)
    edges = direct_edges(relationships)
    exposure_edges = [e for e in edges if e.child_resource_type == "exposure"]

    violations = []
    by_exposure = group_by(exposure_edges, key=lambda e: e.child)
    for exposure_id, parents in by_exposure.items():
        exposure = by_id.get(exposure_id)
        if not exposure:
            continue
        for edge in parents:
            parent = by_id.get(edge.parent)
            if parent and parent.resource_type == "model" and not parent.is_public:
                violations.append(
                    Violation(
                        rule_id=("governance/exposures-depend-on-private-models"),
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
