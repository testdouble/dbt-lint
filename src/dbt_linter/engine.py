"""Rule engine: discovery, filtering, dispatch, exclusion."""

from __future__ import annotations

from fnmatch import fnmatch

from dbt_linter.config import Config, RuleConfig, matches_path_filter
from dbt_linter.loader import load_custom_rules
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import RuleDef, get_all_rules


def evaluate(
    resources: list[Resource],
    relationships: list[Relationship],
    config: Config,
) -> list[Violation]:
    """Run all enabled rules and collect violations."""
    all_rules = get_all_rules() + load_custom_rules(config)
    violations = []
    for rule_def in all_rules:
        rule_config = config.rule_config(rule_def.id)
        if not rule_config.enabled:
            continue

        if rule_def.is_per_resource:
            for resource in resources:
                if _is_excluded(resource, rule_def.id, rule_config, config):
                    continue
                result = rule_def.fn(resource, rule_config)
                if result:
                    violations.append(_finalize(result, rule_def, rule_config))
        else:
            filtered = [
                r
                for r in resources
                if not _is_excluded(r, rule_def.id, rule_config, config)
            ]
            results = rule_def.fn(filtered, relationships, rule_config)
            violations.extend(_finalize(v, rule_def, rule_config) for v in results)

    return violations


def _is_excluded(
    resource: Resource,
    rule_id: str,
    rule_config: RuleConfig,
    config: Config,
) -> bool:
    if rule_id in resource.skip_rules:
        return True
    if any(fnmatch(resource.resource_id, pat) for pat in rule_config.exclude_resources):
        return True
    return not matches_path_filter(resource.file_path, config.include, config.exclude)


def _finalize(
    violation: Violation,
    rule_def: RuleDef,
    rule_config: RuleConfig,
) -> Violation:
    """Ensure violation has rule_id and severity from config."""
    if violation.rule_id and violation.severity:
        return violation
    return Violation(
        rule_id=violation.rule_id or rule_def.id,
        resource_id=violation.resource_id,
        resource_name=violation.resource_name,
        message=violation.message,
        severity=violation.severity or rule_config.severity,
        file_path=violation.file_path,
    )
