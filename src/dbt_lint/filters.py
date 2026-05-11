"""Filter operations for rules, resources, and violations."""

from __future__ import annotations

from fnmatch import fnmatch

from dbt_lint.config import Config, RuleConfig, matches_path_filter
from dbt_lint.models import Resource, Violation
from dbt_lint.rules import RuleDef


def is_resource_excluded_from_rule(
    resource: Resource,
    rule_id: str,
    rule_config: RuleConfig,
    config: Config,
) -> bool:
    """Return True if the resource is excluded from evaluation under this rule."""
    if rule_id in resource.skip_rules:
        return True
    if any(fnmatch(resource.resource_id, pat) for pat in rule_config.exclude_resources):
        return True
    return not matches_path_filter(resource.file_path, config.include, config.exclude)


def filter_violations_by_resource_exclusions(
    violations: list[Violation],
    rule_config: RuleConfig,
) -> list[Violation]:
    """Drop violations whose resource matches the rule's ``exclude_resources`` globs."""
    if not rule_config.exclude_resources:
        return violations
    return [
        violation
        for violation in violations
        if not any(
            fnmatch(violation.resource_id, pat) for pat in rule_config.exclude_resources
        )
    ]


def filter_rules_by_id(
    rules: list[RuleDef],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
) -> list[RuleDef]:
    """Apply ``--select`` / ``--exclude`` rule-ID filters to a rule list."""
    if select:
        rules = [rule for rule in rules if rule.id in select]
    if exclude:
        rules = [rule for rule in rules if rule.id not in exclude]
    return rules


def filter_violations_by_severity(
    violations: list[Violation],
    minimum: str,
) -> list[Violation]:
    """Keep violations at or above the minimum severity threshold.

    ``minimum="warn"`` returns warn and error violations; ``minimum="error"``
    returns only error violations.
    """
    if minimum == "warn":
        return list(violations)
    return [violation for violation in violations if violation.severity == "error"]
