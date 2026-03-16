"""Rule framework: decorator, registry, signature detection, helpers."""

from __future__ import annotations

import inspect
import re
import textwrap
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


def rule(
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
    has_examples: bool


# Sections that get extracted into their own RuleInfo fields.
_NAMED_SECTIONS = re.compile(r"^(Remediation|Exceptions|Examples):", re.MULTILINE)


def _parse_docstring_sections(doc: str) -> dict[str, Any]:
    """Extract named sections from a Google-style docstring.

    Returns a dict with keys: rationale, remediation, exceptions,
    has_examples. Rationale is everything before the first named
    section header. Each named section is dedented body text.
    """
    doc = textwrap.dedent(doc).strip()
    if not doc:
        return {
            "rationale": "",
            "remediation": "",
            "exceptions": "",
            "has_examples": False,
        }

    # Split on section headers, keeping the delimiter.
    parts = _NAMED_SECTIONS.split(doc)

    # parts[0] is everything before the first named section = rationale
    rationale = parts[0].strip()

    sections: dict[str, str] = {}
    # pairs: (header, body) from parts[1:]
    for i in range(1, len(parts), 2):
        header = parts[i].lower()
        body = textwrap.dedent(parts[i + 1]).strip() if i + 1 < len(parts) else ""
        sections[header] = body

    return {
        "rationale": rationale,
        "remediation": sections.get("remediation", ""),
        "exceptions": sections.get("exceptions", ""),
        "has_examples": "examples" in sections,
    }


def generate_rules_index() -> list[RuleInfo]:
    """Build a sorted index of all rules with extracted docstring metadata."""
    rules = get_all_rules()
    index = []
    for r in rules:
        doc = r.fn.__doc__ or ""
        sections = _parse_docstring_sections(doc)
        index.append(
            RuleInfo(
                id=r.id,
                category=r.category,
                description=r.description,
                is_per_resource=r.is_per_resource,
                rationale=sections["rationale"],
                remediation=sections["remediation"],
                exceptions=sections["exceptions"],
                has_examples=sections["has_examples"],
            )
        )
    return sorted(index, key=lambda r: r.id)


def get_all_rules() -> list[RuleDef]:
    """Discover all decorated rule functions across rule modules."""
    from dbt_linter.rules import (
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
