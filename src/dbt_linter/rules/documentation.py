"""Documentation rules: description and coverage checks."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import filter_by_model_type, rule


@rule(
    id="documentation/undocumented-models",
    description="Models without a description.",
)
def undocumented_models(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type == "model" and not resource.is_described:
        return Violation(
            rule_id="documentation/undocumented-models",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=f"{resource.resource_name}: missing description",
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="documentation/undocumented-sources",
    description="Sources without a source-level description.",
)
def undocumented_sources(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "source":
        return None
    # source_description is stored in meta under our convention
    # For sources, is_described tracks the table-level description.
    # Source-level description is tracked via meta["source_description_populated"]
    source_described = resource.meta.get("source_description_populated", True)
    if not source_described:
        return Violation(
            rule_id="documentation/undocumented-sources",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=f"{resource.resource_name}: source missing description",
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="documentation/undocumented-source-tables",
    description="Source tables without a table-level description.",
)
def undocumented_source_tables(
    resource: Resource,
    config: RuleConfig,
) -> Violation | None:
    if resource.resource_type == "source" and not resource.is_described:
        return Violation(
            rule_id="documentation/undocumented-source-tables",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(f"{resource.resource_name}: source table missing description"),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="documentation/documentation-coverage",
    description="Documentation coverage below target, by model type.",
)
def documentation_coverage(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    target = config.params.get("documentation_coverage_target", 100)
    violations = []
    model_types = config.params.get("model_types", [])

    for mt in model_types:
        models = filter_by_model_type(
            [r for r in resources if r.resource_type == "model"], mt
        )
        if not models:
            continue
        described = sum(1 for m in models if m.is_described)
        pct = (described / len(models)) * 100
        if pct < target:
            violations.append(
                Violation(
                    rule_id="documentation/documentation-coverage",
                    resource_id=f"model_type:{mt}",
                    resource_name=mt,
                    message=(f"{mt}: {pct:.0f}% documented (target: {target}%)"),
                    severity=config.severity,
                    file_path="",
                )
            )
    return violations


@rule(
    id="documentation/column-documentation-coverage",
    description="Column documentation coverage below target.",
)
def column_documentation_coverage(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    target = config.params.get("column_documentation_coverage_target")
    if target is None:
        return []

    violations = []
    for resource in resources:
        if resource.resource_type != "model":
            continue
        if not resource.columns:
            continue
        described = sum(1 for c in resource.columns if c.is_described)
        pct = (described / len(resource.columns)) * 100
        if pct < target:
            violations.append(
                Violation.from_resource(
                    resource,
                    f"{resource.resource_name}: {pct:.0f}% columns"
                    f" documented (target: {target}%)",
                )
            )
    return violations
