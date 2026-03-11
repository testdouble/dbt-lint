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
