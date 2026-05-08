"""Private lint pipeline facade. Not exported from dbt_lint."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from dbt_lint.config import (
    Config,
    load_config,
    load_suppressions,
    merge_suppressions,
)
from dbt_lint.engine import evaluate
from dbt_lint.graph import build_relationships
from dbt_lint.manifest import parse_manifest
from dbt_lint.models import Violation
from dbt_lint.registry import Registry
from dbt_lint.rules import RuleDef


class LintError(Exception):
    """Base class for expected lint pipeline failures.

    The CLI catches LintError specifically and exits 2; unexpected
    exceptions propagate so real bugs surface instead of being swallowed.
    """


class ConfigError(LintError):
    """Failure loading config or suppressions (YAML parse, file IO, regex validation)."""


class ManifestError(LintError):
    """Failure parsing the dbt manifest (JSON decode, file IO, schema mismatch)."""


class CustomRuleError(LintError):
    """Failure assembling custom rules (import, validation, missing config dir)."""


@dataclass
class LintResult:
    violations: list[Violation] = field(default_factory=list)
    excluded: int = 0
    resource_counts: dict[str, int] = field(default_factory=dict)


def run(  # noqa: PLR0913
    *,
    manifest_path: Path,
    config_path: Path | None,
    suppressions_path: Path | None,
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    fail_fast: bool,
) -> LintResult:
    """Compose the lint pipeline and return a LintResult.

    None for config_path uses defaults; None for suppressions_path skips merging.
    """
    try:
        config = load_config(config_path)
    except (yaml.YAMLError, OSError, ValueError) as exc:
        raise ConfigError(str(exc)) from exc

    if suppressions_path is not None:
        try:
            config = merge_suppressions(config, load_suppressions(suppressions_path))
        except (yaml.YAMLError, OSError, ValueError) as exc:
            raise ConfigError(str(exc)) from exc

    try:
        resources, edges = parse_manifest(manifest_path, config)
    except (OSError, json.JSONDecodeError, KeyError, ValueError) as exc:
        raise ManifestError(str(exc)) from exc

    relationships = build_relationships(resources, edges)

    try:
        rules = _assemble_rules(config)
    except (ImportError, OSError, ValueError) as exc:
        raise CustomRuleError(str(exc)) from exc

    evaluation = evaluate(
        resources, relationships, config, rules=rules, fail_fast=fail_fast
    )

    violations = _filter_by_rule_id(evaluation.violations, select, exclude)
    resource_counts = dict(Counter(resource.resource_type for resource in resources))

    return LintResult(
        violations=violations,
        excluded=evaluation.excluded,
        resource_counts=resource_counts,
    )


def _assemble_rules(config: Config) -> list[RuleDef]:
    """Build the rule list: built-ins plus any custom rules from config."""
    registry = Registry()
    if config._custom_rule_entries:
        if config.config_dir is None:
            msg = "Custom rules require a config file (source paths are relative)"
            raise ValueError(msg)
        for entry in config._custom_rule_entries:
            registry.register_from_path(entry.source, entry.rule_id, config.config_dir)
    return registry.all()


def _filter_by_rule_id(
    violations: list[Violation],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
) -> list[Violation]:
    """Apply --select / --exclude rule-ID filters to a violation list."""
    if select:
        violations = [
            violation for violation in violations if violation.rule_id in select
        ]
    if exclude:
        violations = [
            violation for violation in violations if violation.rule_id not in exclude
        ]
    return violations
