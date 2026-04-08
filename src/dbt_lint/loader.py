"""Dynamic loading of custom rules from Python files via source directive."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

from dbt_lint.config import Config
from dbt_lint.rules import RuleDef, get_all_rules


def _synthetic_module_name(source_path: Path, config_dir: Path) -> str:
    """Derive a stable synthetic module name from the file path.

    custom_rules/modeling/select_distinct.py
    -> dbt_lint_custom.modeling.select_distinct
    """
    try:
        relative = source_path.relative_to(config_dir)
    except ValueError:
        relative = source_path
    parts = relative.with_suffix("").parts
    return "dbt_lint_custom." + ".".join(parts)


def _import_module(source_path: Path, module_name: str) -> types.ModuleType:
    """Import a Python file as a module with the given synthetic name."""
    if module_name in sys.modules:
        return sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, source_path)
    if spec is None or spec.loader is None:
        msg = f"Could not create module spec for {source_path}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _find_rules_in_module(module) -> list:
    """Find all @rule-decorated functions in a module."""
    return [
        obj
        for obj in vars(module).values()
        if callable(obj) and hasattr(obj, "_rule_meta")
    ]


def load_custom_rules(config: Config) -> list[RuleDef]:
    """Load custom rules from source directives in config.

    Each custom rule entry has a rule_id, source path, and config overrides.
    Returns RuleDef objects that the engine can dispatch identically to
    built-in rules.
    """
    entries = config._custom_rule_entries
    if not entries:
        return []

    config_dir = config.config_dir
    if config_dir is None:
        msg = "Custom rules require a config file (source paths are relative)"
        raise ValueError(msg)

    builtin_ids = {r.id for r in get_all_rules()}
    rules: list[RuleDef] = []

    for entry in entries:
        if entry.rule_id in builtin_ids:
            msg = f"Custom rule {entry.rule_id}: conflicts with built-in rule"
            raise ValueError(msg)

        source_path = (config_dir / entry.source).resolve()
        if not source_path.is_relative_to(config_dir.resolve()):
            msg = (
                f"Custom rule {entry.rule_id}: source path {entry.source}"
                f" resolves outside config directory"
            )
            raise ValueError(msg)
        if not source_path.is_file():
            msg = f"Custom rule {entry.rule_id}: file not found: {entry.source}"
            raise FileNotFoundError(msg)

        module_name = _synthetic_module_name(source_path, config_dir)
        try:
            module = _import_module(source_path, module_name)
        except Exception as exc:
            msg = (
                f"Failed to load custom rule {entry.rule_id} from {entry.source}: {exc}"
            )
            raise ImportError(msg) from exc

        rule_fns = _find_rules_in_module(module)
        if not rule_fns:
            msg = (
                f"Custom rule {entry.rule_id}:"
                f" no @rule function found in {entry.source}"
            )
            raise ValueError(msg)

        matched = [fn for fn in rule_fns if fn._rule_meta.id == entry.rule_id]
        if not matched:
            found_ids = [fn._rule_meta.id for fn in rule_fns]
            msg = (
                f"Custom rule {entry.rule_id}:"
                f' no matching @rule(id="{entry.rule_id}") in file;'
                f" found: {found_ids}"
            )
            raise ValueError(msg)

        rules.append(RuleDef.from_function(matched[0]))

    return rules
