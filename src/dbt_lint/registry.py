"""Rule discovery and registration.

Hosts the Registry class that owns built-in rule discovery. The hard-coded
list of built-in rule modules lives behind a single named seam,
``_builtin_module_sources``, so future discovery modes (pkgutil walk, entry
points) can plug in without changing public methods.
"""

from __future__ import annotations

from types import ModuleType

from dbt_lint.rules import RuleDef


class Registry:
    """Owns built-in rule discovery."""

    def builtins(self) -> list[RuleDef]:
        """Return all built-in rule definitions."""
        rules: list[RuleDef] = []
        for module in self._builtin_module_sources():
            for obj in vars(module).values():
                if callable(obj) and hasattr(obj, "_rule_meta"):
                    rules.append(RuleDef.from_function(obj))
        return rules

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
