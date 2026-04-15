"""Core dataclasses: Resource, Relationship, Violation, DirectEdge, ColumnInfo."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ColumnInfo:
    """A column within a dbt resource, extracted from manifest columns dict."""

    name: str
    data_type: str
    is_described: bool


@dataclass(frozen=True)
class Resource:
    """A dbt node (model, source, or exposure) extracted from manifest.json."""

    resource_id: str
    resource_name: str
    resource_type: str
    file_path: str
    model_type: str
    materialization: str
    schema_name: str
    database: str
    is_described: bool
    is_public: bool
    is_contract_enforced: bool
    hard_coded_references: bool
    number_of_columns: int
    number_of_documented_columns: int
    is_freshness_enabled: bool
    number_of_tests: int
    is_primary_key_tested: bool
    has_relationship_tests: bool
    patch_path: str
    tags: tuple[str, ...]
    meta: dict
    skip_rules: frozenset[str]
    raw_code: str
    config: dict
    columns: tuple[ColumnInfo, ...]


@dataclass(frozen=True)
class Relationship:
    """A dependency edge between two resources, possibly transitive."""

    parent: str
    child: str
    parent_resource_type: str
    child_resource_type: str
    parent_model_type: str
    child_model_type: str
    parent_materialization: str
    parent_is_public: bool
    distance: int
    is_dependent_on_chain_of_views: bool


@dataclass(frozen=True)
class Violation:
    """A single rule violation found during evaluation."""

    rule_id: str
    resource_id: str
    resource_name: str
    message: str
    severity: str
    file_path: str
    patch_path: str = ""

    @classmethod
    def from_resource(cls, resource: Resource, message: str) -> Violation:
        """Create a Violation from a Resource.

        Leaves rule_id and severity empty for the engine to fill.
        """
        return cls(
            rule_id="",
            resource_id=resource.resource_id,
            resource_name=resource.resource_name,
            message=message,
            severity="",
            file_path=resource.file_path,
            patch_path=strip_patch_prefix(resource.patch_path),
        )


def strip_patch_prefix(path: str) -> str:
    """Strip the 'project://' prefix from a dbt patch_path."""
    return path.split("://", 1)[-1] if "://" in path else path


@dataclass(frozen=True)
class DirectEdge:
    """A direct (distance=1) dependency from parent to child. Internal use only."""

    parent: str
    child: str
