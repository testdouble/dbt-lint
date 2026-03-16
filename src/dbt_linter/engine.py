"""Rule engine: discovery, filtering, dispatch, exclusion."""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch

from dbt_linter.config import Config, RuleConfig, matches_path_filter
from dbt_linter.loader import load_custom_rules
from dbt_linter.models import Relationship, Resource, Violation
from dbt_linter.rules import RuleDef, get_all_rules


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
    fail_fast: bool = False,
) -> EvaluationResult:
    """Run all enabled rules and collect violations."""
    all_rules = get_all_rules() + load_custom_rules(config)
    result = EvaluationResult()
    for rule_def in all_rules:
        rule_config = config.rule_config(rule_def.id)
        if not rule_config.enabled:
            continue

        if rule_def.is_per_resource:
            for resource in resources:
                if _is_excluded(resource, rule_def.id, rule_config, config):
                    continue
                violation = rule_def.fn(resource, rule_config)
                if violation:
                    result.violations.append(
                        _finalize(violation, rule_def, rule_config)
                    )
                    if fail_fast:
                        return result
        else:
            filtered = [
                r
                for r in resources
                if not _is_excluded(r, rule_def.id, rule_config, config)
            ]
            raw = rule_def.fn(filtered, relationships, rule_config)
            kept = _post_filter(raw, rule_config)
            result.excluded += len(raw) - len(kept)
            result.violations.extend(_finalize(v, rule_def, rule_config) for v in kept)
            if fail_fast and result.violations:
                return result

    return result


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


def _post_filter(
    violations: list[Violation],
    rule_config: RuleConfig,
) -> list[Violation]:
    """Remove violations for resources excluded via config."""
    if not rule_config.exclude_resources:
        return violations
    return [
        v
        for v in violations
        if not any(fnmatch(v.resource_id, pat) for pat in rule_config.exclude_resources)
    ]


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
