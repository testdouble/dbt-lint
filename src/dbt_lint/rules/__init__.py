"""Rule framework: decorator, registry, signature detection, helpers."""

from __future__ import annotations

import inspect
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, get_type_hints

from dbt_lint.models import Relationship, Resource, Violation, strip_patch_prefix

_PER_RESOURCE_PARAMS = {"resource", "context"}
_AGGREGATE_PARAMS = {"resources", "relationships", "context"}


@dataclass
class RuleContext:
    """Per-rule context handed to rule functions during evaluation.

    Author-visible surfaces: ``params`` (rule-relevant config values) and
    ``violation`` / ``violation_for`` (violation construction).

    ``_rule_id`` and ``_severity`` are populated by the engine and are
    private plumbing used by the violation constructors.
    """

    params: dict[str, Any] = field(default_factory=dict)
    _rule_id: str = ""
    _severity: str = ""

    def violation(self, resource: Resource, message: str) -> Violation:
        """Build a fully-formed Violation from a Resource."""
        return Violation(
            rule_id=self._rule_id,
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=message,
            severity=self._severity,
            file_path=resource.file_path,
            patch_path=strip_patch_prefix(resource.patch_path),
        )

    def violation_for(
        self,
        *,
        resource_id: str,
        resource_name: str,
        message: str,
        file_path: str = "",
        patch_path: str = "",
    ) -> Violation:
        """Build a fully-formed Violation when no Resource is available.

        Used by aggregate rules that emit violations keyed by a synthetic
        identifier (e.g., a model_type bucket) or by edge-walking rules
        whose lookup may miss a Resource.
        """
        return Violation(
            rule_id=self._rule_id,
            resource_id=resource_id,
            resource_name=resource_name,
            message=message,
            severity=self._severity,
            file_path=file_path,
            patch_path=patch_path,
        )


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
        f"@rule error in {rule_id}: expected (resource, context)"
        f" or (resources, relationships, context),"
        f" got ({', '.join(sorted(params))})."
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
    """Summary metadata for a rule, used by generate_rules_index."""

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
    from dbt_lint.registry import Registry  # noqa: PLC0415

    return Registry().builtins()


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


def resolve_name(by_id: dict[str, Resource], resource_id: str) -> str:
    """Return the human-readable name for a resource_id, falling back to the raw ID."""
    resource = by_id.get(resource_id)
    return resource.resource_name if resource else resource_id
