"""Rule framework: decorator, registry, signature detection, helpers."""

from __future__ import annotations

import inspect
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, get_type_hints

from dbt_linter.models import Relationship, Resource

# Valid per-resource params (resource, config) and aggregate (resources,
# relationships, config). We check type hints to distinguish them.
_PER_RESOURCE_PARAMS = {"resource", "config"}
_AGGREGATE_PARAMS = {"resources", "relationships", "config"}


@dataclass(frozen=True)
class RuleMeta:
    """Metadata attached to a rule function by the @rule decorator."""

    id: str
    description: str
    rationale: str = ""
    remediation: str = ""
    exceptions: str = ""
    examples: tuple[str, ...] = ()

    @property
    def category(self) -> str:
        return self.id.split("/")[0]


def _validate_rule_signature(fn, rule_id: str) -> None:
    """Validate that a rule function has a supported signature."""
    params = set(inspect.signature(fn).parameters.keys())
    if params in (_PER_RESOURCE_PARAMS, _AGGREGATE_PARAMS):
        return
    raise TypeError(
        f"@rule error in {rule_id}: expected (resource, config)"
        f" or (resources, relationships, config),"
        f" got ({', '.join(params)})"
    )


def rule(  # noqa: PLR0913
    id: str,
    description: str,
    *,
    rationale: str = "",
    remediation: str = "",
    exceptions: str = "",
    examples: Sequence[str] = (),
):
    """Decorator that attaches RuleMeta to a rule function."""

    def decorator(fn):
        _validate_rule_signature(fn, id)
        fn._rule_meta = RuleMeta(
            id=id,
            description=description,
            rationale=rationale,
            remediation=remediation,
            exceptions=exceptions,
            examples=tuple(examples),
        )
        return fn

    return decorator


@dataclass(frozen=True)
class RuleDef:
    """A resolved rule definition with metadata and dispatch info."""

    id: str
    category: str
    description: str
    fn: Any
    is_per_resource: bool

    @classmethod
    def from_function(cls, fn) -> RuleDef:
        meta: RuleMeta = fn._rule_meta
        hints = get_type_hints(fn)
        per_resource = "resource" in hints and hints["resource"] is Resource
        return cls(
            id=meta.id,
            category=meta.category,
            description=meta.description,
            fn=fn,
            is_per_resource=per_resource,
        )


@dataclass(frozen=True)
class RuleInfo:
    """Summary metadata for a rule, used by generate_rules_index and --list-rules."""

    id: str
    category: str
    description: str
    is_per_resource: bool
    rationale: str
    remediation: str
    exceptions: str
    examples: tuple[str, ...]


def generate_rules_index() -> list[RuleInfo]:
    """Build a sorted index of all rules from structured metadata."""
    rules = get_all_rules()
    index = []
    for rule_def in rules:
        meta: RuleMeta = rule_def.fn._rule_meta
        index.append(
            RuleInfo(
                id=rule_def.id,
                category=rule_def.category,
                description=rule_def.description,
                is_per_resource=rule_def.is_per_resource,
                rationale=meta.rationale,
                remediation=meta.remediation,
                exceptions=meta.exceptions,
                examples=meta.examples,
            )
        )
    return sorted(index, key=lambda r: r.id)


def get_all_rules() -> list[RuleDef]:
    """Discover all decorated rule functions across rule modules."""
    from dbt_linter.rules import (  # noqa: PLC0415
        documentation,
        governance,
        modeling,
        performance,
        structure,
        testing,
    )

    rules = []
    for module in [
        modeling,
        testing,
        documentation,
        structure,
        performance,
        governance,
    ]:
        for obj in vars(module).values():
            if callable(obj) and hasattr(obj, "_rule_meta"):
                rules.append(RuleDef.from_function(obj))
    return rules


def group_by(items, key) -> dict:
    """Group items by key function, return dict of key -> list."""
    result = defaultdict(list)
    for item in items:
        result[key(item)].append(item)
    return dict(result)


def filter_by_model_type(resources: list[Resource], model_type: str) -> list[Resource]:
    """Filter resources to a specific model type."""
    return [r for r in resources if r.model_type == model_type]


def direct_edges(
    relationships: list[Relationship],
) -> list[Relationship]:
    """Filter to distance=1 relationships."""
    return [r for r in relationships if r.distance == 1]


def resources_by_id(resources: list[Resource]) -> dict[str, Resource]:
    """Index resources by resource_id. Last wins on duplicate IDs."""
    return {r.resource_id: r for r in resources}
