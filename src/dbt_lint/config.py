"""YAML config loading, defaults, merging, RuleConfig, and path filtering."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

SUPPRESSIONS_FILENAME = ".dbt-lint-suppressions.yml"


DEFAULTS: dict[str, Any] = {
    # Thresholds
    "documentation_coverage_target": 100,
    "test_coverage_target": 100,
    "models_fanout_threshold": 3,
    "too_many_joins_threshold": 5,
    "chained_views_threshold": 5,
    # Model types and prefixes
    "staging_prefixes": ["stg_"],
    "intermediate_prefixes": ["int_"],
    "marts_prefixes": [],
    "base_prefixes": ["base_"],
    "other_prefixes": ["rpt_"],
    # Directories
    "staging_folder_name": "staging",
    "intermediate_folder_name": "intermediate",
    "marts_folder_name": "marts",
    "base_folder_name": "base",
    # Materialization constraints
    "staging_allowed_materializations": ["view"],
    "intermediate_allowed_materializations": ["ephemeral", "view"],
    "marts_allowed_materializations": ["table", "incremental"],
    # Include/exclude (regex on original_file_path)
    "include": None,
    "exclude": None,
    # Testing
    "primary_key_test_macros": [
        ["dbt.test_unique", "dbt.test_not_null"],
        ["dbt_utils.test_unique_combination_of_columns"],
    ],
    "enforced_primary_key_node_types": ["model"],
    # Column naming conventions (null = disabled)
    "column_naming_conventions": None,
    # Column documentation coverage target (null = disabled)
    "column_documentation_coverage_target": None,
    # Rule overrides
    "rules": {},
}


@dataclass
class RuleConfig:
    """Per-rule configuration built by merging defaults with overrides."""

    enabled: bool = True
    severity: str = "warn"
    exclude_resources: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class CustomRuleEntry:
    """A custom rule entry from config: rule_id + source path + overrides."""

    rule_id: str
    source: str
    overrides: dict[str, Any]


@dataclass
class Config:
    """Top-level configuration with merged params and rule overrides."""

    params: dict[str, Any]
    include: str | None
    exclude: str | None
    config_dir: Path | None
    _rule_overrides: dict[str, dict[str, Any]]
    _custom_rule_entries: list[CustomRuleEntry]

    def rule_config(self, rule_id: str) -> RuleConfig:
        """Build a RuleConfig for a specific rule, merging defaults with overrides."""
        overrides = self._rule_overrides.get(rule_id, {})
        return RuleConfig(
            enabled=overrides.get("enabled", True),
            severity=overrides.get("severity", "warn"),
            exclude_resources=overrides.get("exclude_resources", []),
            params=self.params,
        )


def load_config(path: Path | None) -> Config:
    """Load config from YAML file, falling back to defaults."""
    raw = (yaml.safe_load(path.read_text()) or {}) if path is not None else {}

    params = {**DEFAULTS, **raw}
    rule_overrides = params.pop("rules", {})

    # Separate custom rule entries (those with source:) from built-in overrides.
    # Both types get their overrides stored in all_overrides for rule_config().
    all_overrides: dict[str, dict[str, Any]] = {}
    custom_entries: list[CustomRuleEntry] = []

    for rule_id, rule_cfg in rule_overrides.items():
        if not isinstance(rule_cfg, dict):
            continue
        if "source" in rule_cfg:
            entry_cfg = {k: v for k, v in rule_cfg.items() if k != "source"}
            custom_entries.append(
                CustomRuleEntry(
                    rule_id=rule_id,
                    source=rule_cfg["source"],
                    overrides=entry_cfg,
                )
            )
            all_overrides[rule_id] = entry_cfg
        else:
            all_overrides[rule_id] = rule_cfg

    config_dir = path.parent if path is not None else None

    include = params.get("include")
    exclude = params.get("exclude")
    _validate_regex(include, "include")
    _validate_regex(exclude, "exclude")

    return Config(
        params=params,
        include=include,
        exclude=exclude,
        config_dir=config_dir,
        _rule_overrides=all_overrides,
        _custom_rule_entries=custom_entries,
    )


def merge_suppressions(
    config: Config, suppressions_rules: dict[str, dict[str, Any]]
) -> Config:
    """Return a new Config with suppressions merged in.

    exclude_resources are unioned; enabled: false from suppressions overrides.
    """
    merged_overrides = dict(config._rule_overrides)

    for rule_id, suppressions_entry in suppressions_rules.items():
        existing = merged_overrides.get(rule_id, {})
        merged_entry = dict(existing)

        if "exclude_resources" in suppressions_entry:
            existing_excludes = set(merged_entry.get("exclude_resources", []))
            suppressions_excludes = set(suppressions_entry["exclude_resources"])
            merged_entry["exclude_resources"] = sorted(
                existing_excludes | suppressions_excludes
            )

        if suppressions_entry.get("enabled") is False:
            merged_entry["enabled"] = False

        merged_overrides[rule_id] = merged_entry

    return Config(
        params=config.params,
        include=config.include,
        exclude=config.exclude,
        config_dir=config.config_dir,
        _rule_overrides=merged_overrides,
        _custom_rule_entries=config._custom_rule_entries,
    )


def load_suppressions(path: Path) -> dict[str, dict[str, Any]]:
    """Load a suppressions YAML file and return its rules section.

    Only extracts exclude_resources and enabled keys from each rule entry.
    Other keys are ignored.
    """
    raw = yaml.safe_load(path.read_text()) or {}
    rules = raw.get("rules", {})

    sanitized: dict[str, dict[str, Any]] = {}
    for rule_id, rule_cfg in rules.items():
        if not isinstance(rule_cfg, dict):
            continue
        entry: dict[str, Any] = {}
        if "exclude_resources" in rule_cfg:
            entry["exclude_resources"] = rule_cfg["exclude_resources"]
        if "enabled" in rule_cfg:
            entry["enabled"] = rule_cfg["enabled"]
        if entry:
            sanitized[rule_id] = entry
    return sanitized


def _validate_regex(pattern: str | None, field_name: str) -> None:
    """Validate a regex pattern at config load time."""
    if pattern is None:
        return
    try:
        re.compile(pattern)
    except re.error as exc:
        msg = f"Invalid regex in '{field_name}': {pattern!r} ({exc})"
        raise ValueError(msg) from exc


@lru_cache(maxsize=256)
def _compile(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern)


def matches_path_filter(
    file_path: str, include: str | None, exclude: str | None
) -> bool:
    """Check if a file path passes include/exclude regex filters."""
    if include is not None and not _compile(include).match(file_path):
        return False
    return not (exclude is not None and _compile(exclude).match(file_path))
