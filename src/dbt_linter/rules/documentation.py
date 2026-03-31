"""Documentation rules: description and coverage checks."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import filter_by_model_type, rule


@rule(
    id="documentation/undocumented-models",
    description="Models without a description.",
    rationale=(
        "Every model should have a description in its YAML properties file."
        "\n\n"
        "Descriptions populate the dbt docs site and serve as the primary "
        "reference for consumers. Missing descriptions force consumers to "
        "read SQL to understand what a model represents."
    ),
    remediation=(
        "Add a description in the model's YAML entry. Use {{ doc() }} "
        "with a docs block in a markdown file for longer descriptions. "
        "Prioritize marts models first, then work upstream."
    ),
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
    rationale=(
        "Every source should have a top-level description."
        "\n\n"
        "Source descriptions document the upstream system and table purpose. "
        "Without them, analysts must reverse-engineer meaning from column "
        "names or track down the source system owner."
    ),
    remediation=(
        "Add a description at the source name level in the YAML "
        "properties file (_<dir>__sources.yml)."
    ),
)
def undocumented_sources(resource: Resource, config: RuleConfig) -> Violation | None:
    if resource.resource_type != "source":
        return None
    # is_described tracks table-level; source-level is in meta
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
    rationale=(
        "Every source table should have its own description."
        "\n\n"
        "Table-level descriptions complement the source-level description by "
        "documenting the specific table's contents and grain. This is "
        "distinct from undocumented-sources, which checks the parent source "
        "definition."
    ),
    remediation=(
        "Add a description under the table entry in the source YAML definition."
    ),
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
    rationale=(
        "Documentation coverage should meet a minimum target per model type."
        "\n\n"
        "Tracks the percentage of models with descriptions, broken down by "
        "model type (staging, marts, intermediate, etc.). Reports aggregate "
        "percentages but does not identify individual models. Pair with "
        "documentation/undocumented-models to get an actionable list of "
        "which models need descriptions."
        "\n\n"
        "Useful for incremental adoption: set a lower target initially and "
        "ratchet up over time."
        "\n\n"
        "Configurable via documentation_coverage_target (default: 100) and "
        "model_types (list of model types to check)."
    ),
    remediation=(
        "Add descriptions to undocumented models. Use {{ doc() }} "
        "with markdown docs blocks for longer descriptions. Start "
        "with the lowest-coverage model type to maximize impact."
    ),
)
def documentation_coverage(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    target = config.params.get("documentation_coverage_target", 100)
    violations = []
    model_types = config.params.get("model_types", [])

    for model_type in model_types:
        models = filter_by_model_type(
            [r for r in resources if r.resource_type == "model"], model_type
        )
        if not models:
            continue
        described = sum(1 for m in models if m.is_described)
        pct = (described / len(models)) * 100
        if pct < target:
            violations.append(
                Violation(
                    rule_id="documentation/documentation-coverage",
                    resource_id=f"model_type:{model_type}",
                    resource_name=model_type,
                    message=(
                        f"{model_type}: {pct:.0f}% documented (target: {target}%)"
                    ),
                    severity=config.severity,
                    file_path="",
                )
            )
    return violations


@rule(
    id="documentation/column-documentation-coverage",
    description="Column documentation coverage below target.",
    rationale=(
        "Column documentation coverage should meet a minimum target per "
        "model."
        "\n\n"
        "Flags models where fewer than the configured percentage of columns "
        "have descriptions. Disabled by default (no target set). Useful for "
        "public or marts models where column-level docs are expected."
        "\n\n"
        "Configurable via column_documentation_coverage_target (no default, "
        "rule is inactive until set)."
    ),
    remediation=(
        "Add description to each column entry in the model's YAML "
        "properties file. Focus on public and marts models first."
    ),
    exceptions=(
        "Models with many auto-generated columns (e.g., pivot "
        "outputs) where individual descriptions add little value."
    ),
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
