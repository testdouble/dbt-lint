"""Testing rules: PK tests, freshness, test coverage."""

from __future__ import annotations

from dbt_linter.config import RuleConfig
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import direct_edges, filter_by_model_type, rule


@rule(
    id="testing/missing-primary-key-tests",
    description="Models without primary key uniqueness tests.",
)
def missing_primary_key_tests(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    """Every model should have a unique or unique_combination test on its primary key.

    Primary key tests catch duplicates and nulls that would silently
    corrupt downstream aggregations. Without them, data quality issues
    propagate undetected through the DAG.

    Configurable via enforced_primary_key_node_types (default: ["model"]).

    Remediation:
        Apply unique + not_null tests to the grain column in the
        model's YAML. For composite keys, add a surrogate key or
        use dbt_utils.unique_combination_of_columns.
    """
    enforced_types = config.params.get("enforced_primary_key_node_types", ["model"])
    if resource.resource_type in enforced_types and not resource.is_primary_key_tested:
        return Violation(
            rule_id="testing/missing-primary-key-tests",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(f"{resource.resource_name}: missing primary key test"),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="testing/sources-without-freshness",
    description="Sources without freshness checks configured.",
)
def sources_without_freshness(
    resource: Resource, config: RuleConfig
) -> Violation | None:
    """Every source should have a freshness check configured.

    Freshness checks detect stale upstream data before it silently
    affects downstream models. Without them, a broken pipeline in the
    source system can go unnoticed for days. Source freshness also
    enables the source_status selector method for smart reruns.

    Remediation:
        Add a freshness block with warn_after and/or error_after
        at the source name or table name level in the source YAML.

    Exceptions:
        Static reference data that never changes (e.g., zip code
        mappings, country codes).
    """
    if resource.resource_type == "source" and not resource.is_freshness_enabled:
        return Violation(
            rule_id="testing/sources-without-freshness",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=(f"{resource.resource_name}: no freshness check configured"),
            severity=config.severity,
            file_path=resource.file_path,
        )
    return None


@rule(
    id="testing/missing-relationship-tests",
    description="Models with model parents but no relationship tests.",
)
def missing_relationship_tests(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    """Non-staging models with model parents should have relationship tests.

    Relationship tests validate that foreign key references resolve to
    existing rows. Without them, joins can silently drop rows or
    produce nulls from orphaned keys. Staging models are excluded
    because they typically reference sources, not other models.

    Remediation:
        Add a relationships test on the foreign key column(s) in
        the model's YAML, referencing the parent model and its
        primary key.

    Exceptions:
        Models that only join dimension tables with guaranteed
        referential integrity at the warehouse level.
    """
    edges = direct_edges(relationships)
    models_with_model_parents = {
        e.child
        for e in edges
        if e.child_resource_type == "model" and e.parent_resource_type == "model"
    }

    violations = []
    for r in resources:
        if (
            r.resource_type == "model"
            and r.model_type != "staging"
            and r.resource_id in models_with_model_parents
            and not r.has_relationship_tests
        ):
            violations.append(
                Violation(
                    rule_id="testing/missing-relationship-tests",
                    resource_id=r.resource_id,
                    resource_name=r.resource_name,
                    message=(
                        f"{r.resource_name}: has model dependencies"
                        " but no relationship tests"
                    ),
                    severity=config.severity,
                    file_path=r.file_path,
                )
            )
    return violations


@rule(
    id="testing/test-coverage",
    description="Test coverage below target, by model type.",
)
def check_test_coverage(
    resources: list[Resource],
    relationships: list[Relationship],
    config: RuleConfig,
) -> list[Violation]:
    """Test coverage should meet a minimum target per model type.

    Measures the percentage of models with at least one primary key
    test, broken down by model type. Like documentation-coverage, this
    supports incremental adoption with a ratcheting target.

    Configurable via test_coverage_target (default: 100) and
    model_types (list of model types to check).

    Remediation:
        Apply generic tests in YAML or create singular tests. At
        minimum: unique + not_null on the primary key for each
        untested model.
    """
    target = config.params.get("test_coverage_target", 100)
    violations = []
    model_types = config.params.get("model_types", [])

    for mt in model_types:
        models = filter_by_model_type(
            [r for r in resources if r.resource_type == "model"], mt
        )
        if not models:
            continue
        tested = sum(1 for m in models if m.is_primary_key_tested)
        pct = (tested / len(models)) * 100
        if pct < target:
            violations.append(
                Violation(
                    rule_id="testing/test-coverage",
                    resource_id=f"model_type:{mt}",
                    resource_name=mt,
                    message=(f"{mt}: {pct:.0f}% test coverage (target: {target}%)"),
                    severity=config.severity,
                    file_path="",
                )
            )
    return violations
