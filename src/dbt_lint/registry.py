"""Rule discovery and registration.

Hosts the Registry class that owns built-in rule discovery and custom rule
loading. The hard-coded list of built-in rule modules lives behind a single
named seam, ``_builtin_module_sources``, so future discovery modes (pkgutil
walk, entry points) can plug in without changing public methods.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from types import ModuleType

from dbt_lint.rules import RuleDef


def _synthetic_module_name(source_path: Path, config_dir: Path) -> str:
    """Derive a stable synthetic module name from the file path.

    custom_rules/modeling/select_distinct.py
    -> dbt_lint_custom.custom_rules.modeling.select_distinct
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


def _find_rules_in_module(module: ModuleType) -> list:
    """Find all @rule-decorated functions in a module."""
    return [
        obj
        for obj in vars(module).values()
        if callable(obj) and hasattr(obj, "_rule_meta")
    ]


class Registry:
    """Owns built-in rule discovery and custom-rule loading."""

    def __init__(self) -> None:
        self._customs: list[RuleDef] = []

    def builtins(self) -> list[RuleDef]:
        """Return all built-in rule definitions."""
        rules: list[RuleDef] = []
        for module in self._builtin_module_sources():
            for obj in vars(module).values():
                if callable(obj) and hasattr(obj, "_rule_meta"):
                    rules.append(RuleDef.from_function(obj))
        return rules

    def all(self) -> list[RuleDef]:
        """Return built-ins plus all registered custom rules."""
        return self.builtins() + list(self._customs)

    def register_from_path(
        self,
        path: str | Path,
        rule_id: str,
        config_dir: Path,
    ) -> None:
        """Load a custom rule from a Python file with a synthetic module name.

        ``path`` is resolved relative to ``config_dir``. The resolved path
        must remain inside ``config_dir`` (no traversal escapes).

        Raises:
            ValueError: built-in id collision, path traversal escape, missing
                @rule function, or no @rule with matching ``rule_id``.
            FileNotFoundError: source file does not exist.
            ImportError: module fails to import or @rule signature validation
                fails at decoration time.
        """
        builtin_ids = {r.id for r in self.builtins()}
        if rule_id in builtin_ids:
            msg = f"Custom rule {rule_id}: conflicts with built-in rule"
            raise ValueError(msg)

        resolved_config_dir = config_dir.resolve()
        source_path = (config_dir / path).resolve()
        if not source_path.is_relative_to(resolved_config_dir):
            msg = (
                f"Custom rule {rule_id}: source path {path}"
                f" resolves outside config directory"
            )
            raise ValueError(msg)
        if not source_path.is_file():
            msg = f"Custom rule {rule_id}: file not found: {path}"
            raise FileNotFoundError(msg)

        module_name = _synthetic_module_name(source_path, config_dir)
        try:
            module = _import_module(source_path, module_name)
        except Exception as exc:
            msg = f"Failed to load custom rule {rule_id} from {path}: {exc}"
            raise ImportError(msg) from exc

        rule_fns = _find_rules_in_module(module)
        if not rule_fns:
            msg = f"Custom rule {rule_id}: no @rule function found in {path}"
            raise ValueError(msg)

        matched = [fn for fn in rule_fns if fn._rule_meta.id == rule_id]
        if not matched:
            found_ids = [fn._rule_meta.id for fn in rule_fns]
            msg = (
                f"Custom rule {rule_id}:"
                f' no matching @rule(id="{rule_id}") in file;'
                f" found: {found_ids}"
            )
            raise ValueError(msg)

        self._customs.append(RuleDef.from_function(matched[0]))

    def _builtin_module_sources(self) -> list[ModuleType]:
        """Return the list of built-in rule modules.

        Single named seam for future discovery modes. All callers go
        through ``builtins()``; this method is the only place that knows
        which modules contain built-in rules.
        """
        from dbt_lint.rules import (  # noqa: PLC0415
            documentation,
            governance,
            modeling,
            performance,
            structure,
            testing,
        )

        return [
            modeling,
            testing,
            documentation,
            structure,
            performance,
            governance,
        ]
