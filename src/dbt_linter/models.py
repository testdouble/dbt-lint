"""Core dataclasses: Resource, Relationship, Violation, DirectEdge."""

from dataclasses import dataclass


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
    is_primary_key_tested: bool
    tags: tuple[str, ...]
    meta: dict
    skip_rules: frozenset[str]


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


@dataclass(frozen=True)
class DirectEdge:
    """A direct (distance=1) dependency from parent to child. Internal use only."""

    parent: str
    child: str
