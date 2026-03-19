"""Tests for graph.py: BFS transitive closure and relationship building."""

from __future__ import annotations

import pytest

from dbt_linter.graph import build_relationships
from dbt_linter.models import DirectEdge


class TestEmptyInputs:
    def test_no_resources_no_edges(self):
        assert build_relationships([], []) == []

    def test_resources_no_edges(self, make_resource):
        resources = [
            make_resource(resource_id="model.pkg.a"),
            make_resource(resource_id="model.pkg.b"),
        ]
        assert build_relationships(resources, []) == []

    def test_no_resources_with_edges(self):
        edges = [DirectEdge(parent="model.pkg.a", child="model.pkg.b")]
        assert build_relationships([], edges) == []


class TestSingleEdge:
    def test_single_direct_relationship(self, make_resource):
        a = make_resource(
            resource_id="model.pkg.a", model_type="staging", materialization="table"
        )
        b = make_resource(
            resource_id="model.pkg.b", model_type="marts", materialization="view"
        )
        edges = [DirectEdge(parent="model.pkg.a", child="model.pkg.b")]

        rels = build_relationships([a, b], edges)

        assert len(rels) == 1
        rel = rels[0]
        assert rel.parent == "model.pkg.a"
        assert rel.child == "model.pkg.b"
        assert rel.parent_resource_type == "model"
        assert rel.child_resource_type == "model"
        assert rel.parent_model_type == "staging"
        assert rel.child_model_type == "marts"
        assert rel.parent_materialization == "table"
        assert rel.parent_is_public is False
        assert rel.distance == 1
        assert rel.is_dependent_on_chain_of_views is False


class TestLinearChain:
    """A -> B -> C -> D: produces 6 relationships (all transitive pairs)."""

    @pytest.fixture
    def chain(self, make_resource):
        resources = [
            make_resource(resource_id="model.pkg.a", materialization="table"),
            make_resource(resource_id="model.pkg.b", materialization="view"),
            make_resource(resource_id="model.pkg.c", materialization="view"),
            make_resource(resource_id="model.pkg.d", materialization="table"),
        ]
        edges = [
            DirectEdge(parent="model.pkg.a", child="model.pkg.b"),
            DirectEdge(parent="model.pkg.b", child="model.pkg.c"),
            DirectEdge(parent="model.pkg.c", child="model.pkg.d"),
        ]
        rels = build_relationships(resources, edges)
        return rels, {(r.parent, r.child): r for r in rels}

    def test_count(self, chain):
        rels, _ = chain
        assert len(rels) == 6

    def test_direct_distances(self, chain):
        _, by_pair = chain
        assert by_pair[("model.pkg.a", "model.pkg.b")].distance == 1
        assert by_pair[("model.pkg.b", "model.pkg.c")].distance == 1
        assert by_pair[("model.pkg.c", "model.pkg.d")].distance == 1

    def test_transitive_distances(self, chain):
        _, by_pair = chain
        assert by_pair[("model.pkg.a", "model.pkg.c")].distance == 2
        assert by_pair[("model.pkg.a", "model.pkg.d")].distance == 3
        assert by_pair[("model.pkg.b", "model.pkg.d")].distance == 2


class TestIsolatedNodes:
    def test_node_with_no_edges(self, make_resource):
        a = make_resource(resource_id="model.pkg.a")
        b = make_resource(resource_id="model.pkg.b")
        c = make_resource(resource_id="model.pkg.c")
        edges = [DirectEdge(parent="model.pkg.a", child="model.pkg.b")]

        rels = build_relationships([a, b, c], edges)

        ids_in_rels = {(r.parent, r.child) for r in rels}
        assert ("model.pkg.a", "model.pkg.b") in ids_in_rels
        # c should not appear in any relationship
        assert all(r.parent != "model.pkg.c" and r.child != "model.pkg.c" for r in rels)


class TestMissingResourceInEdge:
    def test_edge_with_unknown_parent_skipped(self, make_resource):
        b = make_resource(resource_id="model.pkg.b")
        edges = [DirectEdge(parent="model.pkg.unknown", child="model.pkg.b")]
        assert build_relationships([b], edges) == []

    def test_edge_with_unknown_child_skipped(self, make_resource):
        a = make_resource(resource_id="model.pkg.a")
        edges = [DirectEdge(parent="model.pkg.a", child="model.pkg.unknown")]
        assert build_relationships([a], edges) == []

    def test_partial_graph_with_valid_and_invalid_edges(self, make_resource):
        a = make_resource(resource_id="model.pkg.a")
        b = make_resource(resource_id="model.pkg.b")
        edges = [
            DirectEdge(parent="model.pkg.a", child="model.pkg.b"),
            DirectEdge(parent="model.pkg.a", child="model.pkg.missing"),
        ]
        rels = build_relationships([a, b], edges)
        assert len(rels) == 1
        assert rels[0].parent == "model.pkg.a"
        assert rels[0].child == "model.pkg.b"


class TestSourceAndExposureTypes:
    def test_source_parent_metadata(self, make_resource):
        src = make_resource(
            resource_id="source.pkg.raw.users",
            resource_type="source",
            model_type="",
            materialization="",
        )
        model = make_resource(
            resource_id="model.pkg.stg_users",
            model_type="staging",
            materialization="view",
        )
        edges = [DirectEdge(parent="source.pkg.raw.users", child="model.pkg.stg_users")]

        rels = build_relationships([src, model], edges)

        assert len(rels) == 1
        rel = rels[0]
        assert rel.parent_resource_type == "source"
        assert rel.child_resource_type == "model"
        assert rel.parent_model_type == ""
        assert rel.parent_materialization == ""
        assert rel.parent_is_public is False

    def test_exposure_child_metadata(self, make_resource):
        model = make_resource(
            resource_id="model.pkg.fct_orders",
            model_type="marts",
            is_public=True,
        )
        exp = make_resource(
            resource_id="exposure.pkg.dashboard",
            resource_type="exposure",
            model_type="",
            materialization="",
        )
        edges = [
            DirectEdge(parent="model.pkg.fct_orders", child="exposure.pkg.dashboard")
        ]

        rels = build_relationships([model, exp], edges)

        assert len(rels) == 1
        rel = rels[0]
        assert rel.parent_resource_type == "model"
        assert rel.child_resource_type == "exposure"
        assert rel.parent_is_public is True
        assert rel.child_model_type == ""


class TestChainOfViews:
    """A(table) -> B(view) -> C(view) -> D(table): chain-of-views for intermediates."""

    @pytest.fixture
    def chain(self, make_resource):
        resources = [
            make_resource(resource_id="model.pkg.a", materialization="table"),
            make_resource(resource_id="model.pkg.b", materialization="view"),
            make_resource(resource_id="model.pkg.c", materialization="view"),
            make_resource(resource_id="model.pkg.d", materialization="table"),
        ]
        edges = [
            DirectEdge(parent="model.pkg.a", child="model.pkg.b"),
            DirectEdge(parent="model.pkg.b", child="model.pkg.c"),
            DirectEdge(parent="model.pkg.c", child="model.pkg.d"),
        ]
        rels = build_relationships(resources, edges)
        return {(r.parent, r.child): r for r in rels}

    def test_distance_1_always_false(self, chain):
        # No intermediates at distance=1.
        a_to_b = chain[("model.pkg.a", "model.pkg.b")]
        b_to_c = chain[("model.pkg.b", "model.pkg.c")]
        c_to_d = chain[("model.pkg.c", "model.pkg.d")]
        assert a_to_b.is_dependent_on_chain_of_views is False
        assert b_to_c.is_dependent_on_chain_of_views is False
        assert c_to_d.is_dependent_on_chain_of_views is False

    def test_a_to_c_intermediates_all_views(self, chain):
        # A -> B(view) -> C: intermediate B is view -> True
        a_to_c = chain[("model.pkg.a", "model.pkg.c")]
        assert a_to_c.is_dependent_on_chain_of_views is True

    def test_a_to_d_intermediate_table_breaks_chain(self, chain):
        # A -> B(view) -> C(view) -> D: intermediates B,C are views -> True
        a_to_d = chain[("model.pkg.a", "model.pkg.d")]
        assert a_to_d.is_dependent_on_chain_of_views is True

    def test_b_to_d_intermediate_view(self, chain):
        # B -> C(view) -> D: intermediate C is view -> True
        b_to_d = chain[("model.pkg.b", "model.pkg.d")]
        assert b_to_d.is_dependent_on_chain_of_views is True


class TestChainOfViewsBroken:
    """A(table) -> B(table) -> C(view) -> D(table): B breaks the chain for A->C."""

    def test_table_intermediate_breaks_chain(self, make_resource):
        a = make_resource(resource_id="model.pkg.a", materialization="table")
        b = make_resource(resource_id="model.pkg.b", materialization="table")
        c = make_resource(resource_id="model.pkg.c", materialization="view")
        d = make_resource(resource_id="model.pkg.d", materialization="table")
        edges = [
            DirectEdge(parent="model.pkg.a", child="model.pkg.b"),
            DirectEdge(parent="model.pkg.b", child="model.pkg.c"),
            DirectEdge(parent="model.pkg.c", child="model.pkg.d"),
        ]
        rels = build_relationships([a, b, c, d], edges)
        by_pair = {(r.parent, r.child): r for r in rels}

        # A -> B(table) -> C: intermediate B is table -> False
        assert (
            by_pair[("model.pkg.a", "model.pkg.c")].is_dependent_on_chain_of_views
            is False
        )
        # A -> B(table) -> C(view) -> D: B is table -> False
        assert (
            by_pair[("model.pkg.a", "model.pkg.d")].is_dependent_on_chain_of_views
            is False
        )
        # B -> C(view) -> D: intermediate C is view -> True
        assert (
            by_pair[("model.pkg.b", "model.pkg.d")].is_dependent_on_chain_of_views
            is True
        )


class TestDiamondDAG:
    r"""Diamond: A -> {B, C} -> D. Tests shortest path and deduplication.

       A
      / \
     B   C
      \ /
       D
    """

    @pytest.fixture
    def diamond(self, make_resource):
        resources = [
            make_resource(resource_id="model.pkg.a", materialization="table"),
            make_resource(resource_id="model.pkg.b", materialization="view"),
            make_resource(resource_id="model.pkg.c", materialization="view"),
            make_resource(resource_id="model.pkg.d", materialization="table"),
        ]
        edges = [
            DirectEdge(parent="model.pkg.a", child="model.pkg.b"),
            DirectEdge(parent="model.pkg.a", child="model.pkg.c"),
            DirectEdge(parent="model.pkg.b", child="model.pkg.d"),
            DirectEdge(parent="model.pkg.c", child="model.pkg.d"),
        ]
        rels = build_relationships(resources, edges)
        return rels, {(r.parent, r.child): r for r in rels}

    def test_relationship_count(self, diamond):
        rels, _ = diamond
        # A->B, A->C, A->D, B->D, C->D = 5 relationships
        assert len(rels) == 5

    def test_a_to_d_shortest_distance_is_2(self, diamond):
        _, by_pair = diamond
        assert by_pair[("model.pkg.a", "model.pkg.d")].distance == 2

    def test_direct_distances(self, diamond):
        _, by_pair = diamond
        assert by_pair[("model.pkg.a", "model.pkg.b")].distance == 1
        assert by_pair[("model.pkg.a", "model.pkg.c")].distance == 1
        assert by_pair[("model.pkg.b", "model.pkg.d")].distance == 1
        assert by_pair[("model.pkg.c", "model.pkg.d")].distance == 1

    def test_a_to_d_chain_of_views(self, diamond):
        _, by_pair = diamond
        # A -> B(view) -> D or A -> C(view) -> D: intermediate is view -> True
        a_to_d = by_pair[("model.pkg.a", "model.pkg.d")]
        assert a_to_d.is_dependent_on_chain_of_views is True

    def test_no_duplicate_relationships(self, diamond):
        rels, _ = diamond
        pairs = [(r.parent, r.child) for r in rels]
        assert len(pairs) == len(set(pairs))


class TestWideFanout:
    """One parent with many direct children."""

    def test_fifty_children(self, make_resource):
        parent = make_resource(resource_id="model.pkg.parent")
        children = [
            make_resource(resource_id=f"model.pkg.child_{i}") for i in range(50)
        ]
        edges = [
            DirectEdge(parent="model.pkg.parent", child=f"model.pkg.child_{i}")
            for i in range(50)
        ]

        rels = build_relationships([parent, *children], edges)

        assert len(rels) == 50
        assert all(r.distance == 1 for r in rels)
        assert all(r.parent == "model.pkg.parent" for r in rels)


class TestDeepChain:
    """Linear chain of 20 nodes: verify distance computation at depth."""

    def test_deep_distances(self, make_resource):
        n = 20
        resources = [make_resource(resource_id=f"model.pkg.n{i}") for i in range(n)]
        edges = [
            DirectEdge(parent=f"model.pkg.n{i}", child=f"model.pkg.n{i + 1}")
            for i in range(n - 1)
        ]

        rels = build_relationships(resources, edges)
        by_pair = {(r.parent, r.child): r for r in rels}

        # Total relationships: n*(n-1)/2 = 190
        assert len(rels) == n * (n - 1) // 2

        # First to last should be distance n-1
        assert by_pair[("model.pkg.n0", f"model.pkg.n{n - 1}")].distance == n - 1

        # Spot check a few intermediate distances
        assert by_pair[("model.pkg.n0", "model.pkg.n5")].distance == 5
        assert by_pair[("model.pkg.n10", "model.pkg.n15")].distance == 5


class TestDiamondChainOfViewsShortestPath:
    r"""Diamond where one path has views and other has a table.

       A
      / \
     B   C     (B=view, C=table)
      \ /
       D

    BFS discovers D via whichever path comes first. The chain-of-views
    flag should reflect the shortest path taken by BFS.
    """

    def test_mixed_paths(self, make_resource):
        a = make_resource(resource_id="model.pkg.a", materialization="table")
        b = make_resource(resource_id="model.pkg.b", materialization="view")
        c = make_resource(resource_id="model.pkg.c", materialization="table")
        d = make_resource(resource_id="model.pkg.d", materialization="table")
        # B listed before C in adjacency, so BFS from A visits B first.
        edges = [
            DirectEdge(parent="model.pkg.a", child="model.pkg.b"),
            DirectEdge(parent="model.pkg.a", child="model.pkg.c"),
            DirectEdge(parent="model.pkg.b", child="model.pkg.d"),
            DirectEdge(parent="model.pkg.c", child="model.pkg.d"),
        ]

        rels = build_relationships([a, b, c, d], edges)
        by_pair = {(r.parent, r.child): r for r in rels}

        # A -> D distance is 2 regardless of path
        assert by_pair[("model.pkg.a", "model.pkg.d")].distance == 2
