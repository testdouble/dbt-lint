"""Rule engine: pure dispatch over an explicit rule list."""

from __future__ import annotations

from dataclasses import dataclass, field

from dbt_lint.config import Config
from dbt_lint.filters import (
    filter_violations_by_resource_exclusions,
    is_resource_excluded_from_rule,
)
from dbt_lint.models import Relationship, Resource, Violation
from dbt_lint.rules import RuleContext, RuleDef


@dataclass
class EvaluationResult:
    """Result of running the rule engine."""

    violations: list[Violation] = field(default_factory=list)
    excluded: int = 0


def evaluate(
    resources: list[Resource],
    relationships: list[Relationship],
    config: Config,
    *,
    rules: list[RuleDef],
    fail_fast: bool = False,
) -> EvaluationResult:
    """Run the supplied rules against resources and collect violations."""
    result = EvaluationResult()
    for rule_def in rules:
        rule_config = config.rule_config(rule_def.id)
        if not rule_config.enabled:
            continue

        context = RuleContext(
            params=rule_config.params,
            _rule_id=rule_def.id,
            _severity=rule_config.severity,
        )

        if rule_def.is_per_resource:
            for resource in resources:
                if is_resource_excluded_from_rule(
                    resource, rule_def.id, rule_config, config
                ):
                    continue
                violation = rule_def.fn(resource, context)
                if violation:
                    result.violations.append(violation)
                    if fail_fast:
                        return result
        else:
            eligible = [
                r
                for r in resources
                if not is_resource_excluded_from_rule(
                    r, rule_def.id, rule_config, config
                )
            ]
            raw = rule_def.fn(eligible, relationships, context)
            kept = filter_violations_by_resource_exclusions(raw, rule_config)
            result.excluded += len(raw) - len(kept)
            result.violations.extend(kept)
            if fail_fast and result.violations:
                return result

    return result
