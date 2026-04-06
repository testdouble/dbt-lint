"""Materialization rules: staging, intermediate, marts materialization constraints."""

from __future__ import annotations

from dbt_lint.config import RuleConfig
from dbt_lint.models import Resource, Violation
from dbt_lint.rules import rule


def _check_materialization(
    resource: Resource,
    config: RuleConfig,
    model_type: str,
    rule_id: str,
) -> Violation | None:
    if resource.resource_type != "model" or resource.model_type != model_type:
        return None
    allowed_key = f"{model_type}_allowed_materializations"
    allowed = config.params.get(allowed_key, [])
    if not allowed:
        return None
    if resource.materialization not in allowed:
        return Violation(
            rule_id=rule_id,
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(
                f"{resource.resource_name}:"
                f" {resource.materialization} not allowed"
                f" for {model_type} (allowed: {allowed})"
            ),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="structure/staging-materialization",
    description="Staging model not in allowed materializations.",
    rationale=(
        "Staging models should use allowed materializations (typically view)."
        "\n\n"
        "Staging models are lightweight transformations (renaming, casting) "
        "of source data. Views avoid redundant storage and ensure "
        "downstream models always get fresh data."
        "\n\n"
        "Configurable via staging_allowed_materializations."
    ),
    remediation=(
        "Set the materialization to view (recommended default) in "
        "dbt_project.yml at the staging directory level."
    ),
    exceptions=(
        "High-volume sources where view performance is unacceptable. "
        "Use table or incremental with explicit justification."
    ),
)
def staging_materialization(resource: Resource, config: RuleConfig) -> Violation | None:
    return _check_materialization(
        resource, config, "staging", "structure/staging-materialization"
    )


@rule(
    id="structure/intermediate-materialization",
    description="Intermediate model not in allowed materializations.",
    rationale=(
        "Intermediate models should use allowed materializations."
        "\n\n"
        "Intermediates sit between staging and marts. Ephemeral is the "
        "default recommendation; view in a custom schema for debugging. "
        "Table if the intermediate is reused by many downstream models."
        "\n\n"
        "Configurable via intermediate_allowed_materializations."
    ),
    remediation=(
        "Set the materialization to ephemeral (recommended default) "
        "in dbt_project.yml at the intermediate directory level."
    ),
    exceptions=(
        "Intermediates reused by many downstream models where "
        "ephemeral would cause redundant computation."
    ),
)
def intermediate_materialization(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    return _check_materialization(
        resource,
        config,
        "intermediate",
        "structure/intermediate-materialization",
    )


@rule(
    id="structure/marts-materialization",
    description="Marts model not in allowed materializations.",
    rationale=(
        "Marts models should use allowed materializations (typically table)."
        "\n\n"
        "Marts are the consumption layer queried by BI tools and analysts. "
        "Tables provide stable query performance; incremental for large "
        "fact tables."
        "\n\n"
        "Configurable via marts_allowed_materializations."
    ),
    remediation=(
        "Set the materialization to table (recommended default) or "
        "incremental for large fact tables in dbt_project.yml at "
        "the marts directory level."
    ),
)
def marts_materialization(resource: Resource, config: RuleConfig) -> Violation | None:
    return _check_materialization(
        resource, config, "marts", "structure/marts-materialization"
    )
