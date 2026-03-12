"""YAML config loading, defaults, merging, RuleConfig, and path filtering."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

DEFAULTS: dict[str, Any] = {
    # Thresholds
    "documentation_coverage_target": 100,
    "test_coverage_target": 100,
    "models_fanout_threshold": 3,
    "too_many_joins_threshold": 5,
    "chained_views_threshold": 5,
    # Model types and prefixes
    "model_types": ["base", "staging", "intermediate", "marts", "other"],
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
    "exclude_packages": [],
    # Testing
    "primary_key_test_macros": [
        ["dbt.test_unique", "dbt.test_not_null"],
        ["dbt_utils.test_unique_combination_of_columns"],
    ],
    "enforced_primary_key_node_types": ["model"],
    # Column naming conventions (null = disabled)
    "column_naming_conventions": None,
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
class Config:
    """Top-level configuration with merged params and rule overrides."""

    params: dict[str, Any]
    include: str | None
    exclude: str | None
    _rule_overrides: dict[str, dict[str, Any]]

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

    merged = {**DEFAULTS, **raw}
    rule_overrides = merged.pop("rules", {})
    merged["rules"] = {}  # Keep rules key empty in params

    return Config(
        params=merged,
        include=merged.get("include"),
        exclude=merged.get("exclude"),
        _rule_overrides=rule_overrides,
    )


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
