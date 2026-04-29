"""Rule engine: pure dispatch over an explicit rule list."""

from __future__ import annotations

from dataclasses import dataclass, field
from fnmatch import fnmatch

from dbt_lint.config import Config, RuleConfig, matches_path_filter
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
                if _is_excluded(resource, rule_def.id, rule_config, config):
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
                if not _is_excluded(r, rule_def.id, rule_config, config)
            ]
            raw = rule_def.fn(eligible, relationships, context)
            kept = _post_filter(raw, rule_config)
            result.excluded += len(raw) - len(kept)
            result.violations.extend(kept)
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
