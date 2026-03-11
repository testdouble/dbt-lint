"""Tests for modeling rules."""

from dbt_linter.rules.modeling import (
    direct_join_to_source,
    downstream_depends_on_source,
    duplicate_mart_concepts,
    duplicate_sources,
    hard_coded_references,
    intermediate_fanout,
    model_fanout,
    multiple_sources_joined,
    rejoining_upstream_concepts,
    root_models,
    source_fanout,
    staging_depends_on_downstream,
    staging_depends_on_staging,
    staging_model_too_many_parents,
    too_many_joins,
    unused_sources,
)


class TestDirectJoinToSource:
    def test_flags_model_with_source_and_model_parents(
        self, make_resource, make_relationship, default_config
    ):
        child = make_resource(
            resource_id="model.pkg.m",
            resource_type="model",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.m",
                parent_resource_type="source",
                child_resource_type="model",
            ),
            make_relationship(
                parent="model.pkg.stg",
                child="model.pkg.m",
                parent_resource_type="model",
                child_resource_type="model",
            ),
        ]
        vs = direct_join_to_source(
            [child], rels, default_config
        )
        assert len(vs) == 1

    def test_clean_when_only_model_parents(
        self, make_resource, make_relationship, default_config
    ):
        child = make_resource(resource_id="model.pkg.m")
        rels = [
            make_relationship(
                parent="model.pkg.a",
                child="model.pkg.m",
                parent_resource_type="model",
                child_resource_type="model",
            ),
        ]
        assert direct_join_to_source(
            [child], rels, default_config
        ) == []


class TestDownstreamDependsOnSource:
    def test_flags_marts_depending_on_source(
        self, make_resource, make_relationship, default_config
    ):
        m = make_resource(resource_id="model.pkg.fct")
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.fct",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="marts",
            ),
        ]
        vs = downstream_depends_on_source(
            [m], rels, default_config
        )
        assert len(vs) == 1

    def test_clean_for_staging(
        self, make_resource, make_relationship, default_config
    ):
        m = make_resource(resource_id="model.pkg.stg")
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.stg",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="staging",
            ),
        ]
        assert downstream_depends_on_source(
            [m], rels, default_config
        ) == []


class TestStagingDependsOnStaging:
    def test_flags_staging_to_staging(
        self, make_resource, make_relationship, default_config
    ):
        rels = [
            make_relationship(
                parent="model.pkg.stg_a",
                child="model.pkg.stg_b",
                parent_model_type="staging",
                child_model_type="staging",
            ),
        ]
        vs = staging_depends_on_staging([], rels, default_config)
        assert len(vs) == 1


class TestStagingDependsOnDownstream:
    def test_flags_staging_depending_on_marts(
        self, make_resource, make_relationship, default_config
    ):
        rels = [
            make_relationship(
                parent="model.pkg.fct_x",
                child="model.pkg.stg_y",
                parent_model_type="marts",
                child_model_type="staging",
            ),
        ]
        vs = staging_depends_on_downstream(
            [], rels, default_config
        )
        assert len(vs) == 1


class TestRootModels:
    def test_flags_model_with_no_parents(
        self, make_resource, make_relationship, default_config
    ):
        orphan = make_resource(
            resource_id="model.pkg.orphan",
            resource_type="model",
        )
        vs = root_models([orphan], [], default_config)
        assert len(vs) == 1

    def test_clean_when_model_has_parent(
        self, make_resource, make_relationship, default_config
    ):
        m = make_resource(
            resource_id="model.pkg.m", resource_type="model"
        )
        rels = [
            make_relationship(
                parent="source.pkg.s",
                child="model.pkg.m",
                child_resource_type="model",
            ),
        ]
        vs = root_models([m], rels, default_config)
        assert len(vs) == 0


class TestHardCodedReferences:
    def test_flags_hard_coded(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model", hard_coded_references=True
        )
        assert hard_coded_references(r, default_config) is not None

    def test_clean(self, make_resource, default_config):
        r = make_resource(
            resource_type="model", hard_coded_references=False
        )
        assert hard_coded_references(r, default_config) is None

    def test_ignores_non_models(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source", hard_coded_references=True
        )
        assert hard_coded_references(r, default_config) is None


class TestDuplicateSources:
    def test_flags_duplicates(
        self, make_resource, default_config
    ):
        s1 = make_resource(
            resource_id="source.a.raw.orders",
            resource_type="source",
            resource_name="orders",
            database="db",
            schema_name="raw",
        )
        s2 = make_resource(
            resource_id="source.b.raw.orders",
            resource_type="source",
            resource_name="orders",
            database="db",
            schema_name="raw",
        )
        vs = duplicate_sources([s1, s2], [], default_config)
        assert len(vs) == 1

    def test_clean_when_different_tables(
        self, make_resource, default_config
    ):
        s1 = make_resource(
            resource_type="source",
            resource_name="orders",
            database="db",
            schema_name="raw",
        )
        s2 = make_resource(
            resource_type="source",
            resource_name="customers",
            database="db",
            schema_name="raw",
        )
        assert duplicate_sources([s1, s2], [], default_config) == []


class TestUnusedSources:
    def test_flags_source_with_no_children(
        self, make_resource, default_config
    ):
        s = make_resource(
            resource_id="source.pkg.raw.t",
            resource_type="source",
        )
        vs = unused_sources([s], [], default_config)
        assert len(vs) == 1

    def test_clean_when_source_has_child(
        self, make_resource, make_relationship, default_config
    ):
        s = make_resource(
            resource_id="source.pkg.raw.t",
            resource_type="source",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.stg",
                parent_resource_type="source",
            ),
        ]
        assert unused_sources([s], rels, default_config) == []


class TestMultipleSourcesJoined:
    def test_flags_model_with_two_source_parents(
        self, make_resource, make_relationship, default_config
    ):
        m = make_resource(resource_id="model.pkg.m")
        rels = [
            make_relationship(
                parent="source.pkg.a",
                child="model.pkg.m",
                parent_resource_type="source",
                child_resource_type="model",
            ),
            make_relationship(
                parent="source.pkg.b",
                child="model.pkg.m",
                parent_resource_type="source",
                child_resource_type="model",
            ),
        ]
        vs = multiple_sources_joined([m], rels, default_config)
        assert len(vs) == 1


class TestSourceFanout:
    def test_flags_source_with_multiple_children(
        self, make_resource, make_relationship, default_config
    ):
        s = make_resource(
            resource_id="source.pkg.raw.t",
            resource_type="source",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.a",
                parent_resource_type="source",
            ),
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.b",
                parent_resource_type="source",
            ),
        ]
        vs = source_fanout([s], rels, default_config)
        assert len(vs) == 1


class TestModelFanout:
    def test_flags_model_exceeding_threshold(
        self, make_resource, make_relationship, default_config
    ):
        parent = make_resource(resource_id="model.pkg.hub")
        rels = [
            make_relationship(
                parent="model.pkg.hub",
                child=f"model.pkg.child_{i}",
                parent_resource_type="model",
            )
            for i in range(3)  # default threshold is 3
        ]
        vs = model_fanout([parent], rels, default_config)
        assert len(vs) == 1

    def test_clean_below_threshold(
        self, make_resource, make_relationship, default_config
    ):
        parent = make_resource(resource_id="model.pkg.hub")
        rels = [
            make_relationship(
                parent="model.pkg.hub",
                child="model.pkg.c1",
                parent_resource_type="model",
            ),
            make_relationship(
                parent="model.pkg.hub",
                child="model.pkg.c2",
                parent_resource_type="model",
            ),
        ]
        assert model_fanout([parent], rels, default_config) == []


class TestTooManyJoins:
    def test_flags_model_with_many_parents(
        self, make_resource, make_relationship, default_config
    ):
        child = make_resource(resource_id="model.pkg.wide")
        rels = [
            make_relationship(
                parent=f"model.pkg.p_{i}",
                child="model.pkg.wide",
                child_resource_type="model",
            )
            for i in range(5)  # default threshold is 5
        ]
        vs = too_many_joins([child], rels, default_config)
        assert len(vs) == 1

    def test_clean_below_threshold(
        self, make_resource, make_relationship, default_config
    ):
        child = make_resource(resource_id="model.pkg.m")
        rels = [
            make_relationship(
                parent="model.pkg.p1",
                child="model.pkg.m",
                child_resource_type="model",
            ),
        ]
        assert too_many_joins([child], rels, default_config) == []


class TestRejoiningUpstreamConcepts:
    def test_flags_triad(
        self, make_resource, make_relationship, default_config
    ):
        """A -> B -> C and A -> C: C rejoins A."""
        rels = [
            make_relationship(
                parent="model.pkg.a",
                child="model.pkg.b",
                parent_resource_type="model",
                child_resource_type="model",
            ),
            make_relationship(
                parent="model.pkg.b",
                child="model.pkg.c",
                parent_resource_type="model",
                child_resource_type="model",
            ),
            make_relationship(
                parent="model.pkg.a",
                child="model.pkg.c",
                parent_resource_type="model",
                child_resource_type="model",
            ),
        ]
        c = make_resource(resource_id="model.pkg.c")
        vs = rejoining_upstream_concepts(
            [c], rels, default_config
        )
        assert len(vs) == 1
        assert "rejoins" in vs[0].message

    def test_clean_no_triad(
        self, make_resource, make_relationship, default_config
    ):
        rels = [
            make_relationship(
                parent="model.pkg.a",
                child="model.pkg.b",
            ),
            make_relationship(
                parent="model.pkg.b",
                child="model.pkg.c",
            ),
        ]
        assert rejoining_upstream_concepts(
            [], rels, default_config
        ) == []


class TestStagingModelTooManyParents:
    def test_flags_staging_with_two_parents(
        self, make_resource, make_relationship, default_config
    ):
        stg = make_resource(
            resource_id="model.pkg.stg_orders",
            resource_type="model",
            model_type="staging",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.orders",
                child="model.pkg.stg_orders",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="staging",
            ),
            make_relationship(
                parent="source.pkg.raw.deletes",
                child="model.pkg.stg_orders",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="staging",
            ),
        ]
        vs = staging_model_too_many_parents(
            [stg], rels, default_config
        )
        assert len(vs) == 1
        assert "2 parents" in vs[0].message

    def test_clean_staging_with_one_parent(
        self, make_resource, make_relationship, default_config
    ):
        stg = make_resource(
            resource_id="model.pkg.stg_orders",
            resource_type="model",
            model_type="staging",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.orders",
                child="model.pkg.stg_orders",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="staging",
            ),
        ]
        assert staging_model_too_many_parents(
            [stg], rels, default_config
        ) == []

    def test_ignores_non_staging_models(
        self, make_resource, make_relationship, default_config
    ):
        mart = make_resource(
            resource_id="model.pkg.fct_orders",
            resource_type="model",
            model_type="marts",
        )
        rels = [
            make_relationship(
                parent="model.pkg.a",
                child="model.pkg.fct_orders",
                child_resource_type="model",
                child_model_type="marts",
            ),
            make_relationship(
                parent="model.pkg.b",
                child="model.pkg.fct_orders",
                child_resource_type="model",
                child_model_type="marts",
            ),
        ]
        assert staging_model_too_many_parents(
            [mart], rels, default_config
        ) == []

    def test_respects_custom_threshold(
        self, make_resource, make_relationship, default_config
    ):
        """Base models may need joins (e.g., union + delete). Allow override."""
        default_config.params["staging_max_parents"] = 2
        stg = make_resource(
            resource_id="model.pkg.stg_orders",
            resource_type="model",
            model_type="staging",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.orders",
                child="model.pkg.stg_orders",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="staging",
            ),
            make_relationship(
                parent="source.pkg.raw.deletes",
                child="model.pkg.stg_orders",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="staging",
            ),
        ]
        assert staging_model_too_many_parents(
            [stg], rels, default_config
        ) == []


class TestIntermediateFanout:
    def test_flags_intermediate_with_multiple_children(
        self, make_resource, make_relationship, default_config
    ):
        inter = make_resource(
            resource_id="model.pkg.int_orders_pivoted",
            resource_type="model",
            model_type="intermediate",
        )
        rels = [
            make_relationship(
                parent="model.pkg.int_orders_pivoted",
                child="model.pkg.fct_orders",
                parent_resource_type="model",
                parent_model_type="intermediate",
            ),
            make_relationship(
                parent="model.pkg.int_orders_pivoted",
                child="model.pkg.fct_revenue",
                parent_resource_type="model",
                parent_model_type="intermediate",
            ),
        ]
        vs = intermediate_fanout([inter], rels, default_config)
        assert len(vs) == 1
        assert "2 dependents" in vs[0].message

    def test_clean_intermediate_with_one_child(
        self, make_resource, make_relationship, default_config
    ):
        inter = make_resource(
            resource_id="model.pkg.int_orders_pivoted",
            resource_type="model",
            model_type="intermediate",
        )
        rels = [
            make_relationship(
                parent="model.pkg.int_orders_pivoted",
                child="model.pkg.fct_orders",
                parent_resource_type="model",
                parent_model_type="intermediate",
            ),
        ]
        assert intermediate_fanout([inter], rels, default_config) == []

    def test_ignores_non_intermediate(
        self, make_resource, make_relationship, default_config
    ):
        mart = make_resource(
            resource_id="model.pkg.fct_orders",
            resource_type="model",
            model_type="marts",
        )
        rels = [
            make_relationship(
                parent="model.pkg.fct_orders",
                child="model.pkg.rpt_a",
                parent_resource_type="model",
                parent_model_type="marts",
            ),
            make_relationship(
                parent="model.pkg.fct_orders",
                child="model.pkg.rpt_b",
                parent_resource_type="model",
                parent_model_type="marts",
            ),
        ]
        assert intermediate_fanout([mart], rels, default_config) == []

    def test_respects_custom_threshold(
        self, make_resource, make_relationship, default_config
    ):
        default_config.params["intermediate_fanout_threshold"] = 3
        inter = make_resource(
            resource_id="model.pkg.int_x",
            resource_type="model",
            model_type="intermediate",
        )
        rels = [
            make_relationship(
                parent="model.pkg.int_x",
                child=f"model.pkg.c_{i}",
                parent_resource_type="model",
                parent_model_type="intermediate",
            )
            for i in range(2)
        ]
        assert intermediate_fanout([inter], rels, default_config) == []


class TestDuplicateMartConcepts:
    def test_flags_same_entity_in_different_dirs(
        self, make_resource, default_config
    ):
        """finance/fct_orders and marketing/fct_orders are duplicates."""
        resources = [
            make_resource(
                resource_id="model.pkg.finance_orders",
                resource_type="model",
                model_type="marts",
                resource_name="fct_orders",
                file_path="models/marts/finance/fct_orders.sql",
            ),
            make_resource(
                resource_id="model.pkg.marketing_orders",
                resource_type="model",
                model_type="marts",
                resource_name="fct_orders",
                file_path="models/marts/marketing/fct_orders.sql",
            ),
        ]
        vs = duplicate_mart_concepts(resources, [], default_config)
        assert len(vs) == 1

    def test_flags_same_entity_different_prefix(
        self, make_resource, default_config
    ):
        """Plain name duplicates across dirs are flagged."""
        resources = [
            make_resource(
                resource_id="model.pkg.fin_orders",
                resource_type="model",
                model_type="marts",
                resource_name="orders",
                file_path="models/marts/finance/orders.sql",
            ),
            make_resource(
                resource_id="model.pkg.mkt_orders",
                resource_type="model",
                model_type="marts",
                resource_name="orders",
                file_path="models/marts/marketing/orders.sql",
            ),
        ]
        vs = duplicate_mart_concepts(resources, [], default_config)
        assert len(vs) == 1

    def test_clean_single_instance(self, make_resource, default_config):
        """A single model is not a duplicate."""
        resources = [
            make_resource(
                resource_type="model",
                model_type="marts",
                resource_name="orders",
                file_path="models/marts/finance/orders.sql",
            ),
        ]
        vs = duplicate_mart_concepts(resources, [], default_config)
        assert vs == []

    def test_clean_different_entities(self, make_resource, default_config):
        """Different entities in different dirs are not duplicates."""
        resources = [
            make_resource(
                resource_type="model",
                model_type="marts",
                resource_name="orders",
                file_path="models/marts/finance/orders.sql",
            ),
            make_resource(
                resource_type="model",
                model_type="marts",
                resource_name="customers",
                file_path="models/marts/marketing/customers.sql",
            ),
        ]
        vs = duplicate_mart_concepts(resources, [], default_config)
        assert vs == []

    def test_ignores_non_marts(self, make_resource, default_config):
        resources = [
            make_resource(
                resource_type="model",
                model_type="staging",
                resource_name="stg_stripe__orders",
                file_path="models/staging/stripe/stg_stripe__orders.sql",
            ),
            make_resource(
                resource_type="model",
                model_type="staging",
                resource_name="stg_stripe__orders",
                file_path="models/staging/shopify/stg_stripe__orders.sql",
            ),
        ]
        vs = duplicate_mart_concepts(resources, [], default_config)
        assert vs == []
