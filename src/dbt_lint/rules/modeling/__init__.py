"""Modeling rules: DAG structure, layer boundaries, dependency hygiene."""

from dbt_lint.rules.modeling.dependency_hygiene import (
    duplicate_sources,
    hard_coded_references,
    multiple_sources_joined,
    unused_sources,
)
from dbt_lint.rules.modeling.graph_structure import (
    duplicate_mart_concepts,
    intermediate_fanout,
    mart_depends_on_mart,
    model_fanout,
    rejoining_upstream_concepts,
    source_fanout,
    staging_model_too_many_parents,
    too_many_joins,
)
from dbt_lint.rules.modeling.layer_boundaries import (
    direct_join_to_source,
    downstream_depends_on_source,
    root_models,
    staging_depends_on_downstream,
    staging_depends_on_staging,
)

__all__ = [
    "direct_join_to_source",
    "downstream_depends_on_source",
    "duplicate_mart_concepts",
    "duplicate_sources",
    "hard_coded_references",
    "intermediate_fanout",
    "mart_depends_on_mart",
    "model_fanout",
    "multiple_sources_joined",
    "rejoining_upstream_concepts",
    "root_models",
    "source_fanout",
    "staging_depends_on_downstream",
    "staging_depends_on_staging",
    "staging_model_too_many_parents",
    "too_many_joins",
    "unused_sources",
]
