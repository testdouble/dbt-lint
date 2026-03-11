"""Tests for performance rules."""

from dbt_linter.rules.performance import (
    chained_views,
    exposure_parent_materializations,
)


class TestChainedViews:
    def test_flags_chain_exceeding_threshold(
        self, make_resource, make_relationship, default_config
    ):
        child = make_resource(resource_id="model.pkg.deep")
        rels = [
            make_relationship(
                parent="model.pkg.root",
                child="model.pkg.deep",
                distance=6,
                is_dependent_on_chain_of_views=True,
            ),
        ]
        vs = chained_views([child], rels, default_config)
        assert len(vs) == 1
        assert "depth 6" in vs[0].message

    def test_clean_below_threshold(
        self, make_resource, make_relationship, default_config
    ):
        rels = [
            make_relationship(
                distance=3,
                is_dependent_on_chain_of_views=True,
            ),
        ]
        assert chained_views([], rels, default_config) == []

    def test_clean_non_view_chain(
        self, make_resource, make_relationship, default_config
    ):
        rels = [
            make_relationship(
                distance=6,
                is_dependent_on_chain_of_views=False,
            ),
        ]
        assert chained_views([], rels, default_config) == []


class TestExposureParentMaterializations:
    def test_flags_exposure_depending_on_view(
        self, make_resource, make_relationship, default_config
    ):
        exposure = make_resource(
            resource_id="exposure.pkg.dash",
            resource_type="exposure",
            model_type="",
        )
        parent = make_resource(
            resource_id="model.pkg.v",
            resource_type="model",
            materialization="view",
            resource_name="v",
        )
        rels = [
            make_relationship(
                parent="model.pkg.v",
                child="exposure.pkg.dash",
                parent_resource_type="model",
                child_resource_type="exposure",
            ),
        ]
        vs = exposure_parent_materializations([exposure, parent], rels, default_config)
        assert len(vs) == 1

    def test_flags_exposure_depending_on_source(
        self, make_resource, make_relationship, default_config
    ):
        exposure = make_resource(
            resource_id="exposure.pkg.dash",
            resource_type="exposure",
            model_type="",
        )
        src = make_resource(
            resource_id="source.pkg.raw.t",
            resource_type="source",
            materialization="",
            resource_name="t",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="exposure.pkg.dash",
                parent_resource_type="source",
                child_resource_type="exposure",
            ),
        ]
        vs = exposure_parent_materializations([exposure, src], rels, default_config)
        assert len(vs) == 1

    def test_clean_exposure_depending_on_table(
        self, make_resource, make_relationship, default_config
    ):
        exposure = make_resource(
            resource_id="exposure.pkg.dash",
            resource_type="exposure",
            model_type="",
        )
        parent = make_resource(
            resource_id="model.pkg.fct",
            resource_type="model",
            materialization="table",
        )
        rels = [
            make_relationship(
                parent="model.pkg.fct",
                child="exposure.pkg.dash",
                parent_resource_type="model",
                child_resource_type="exposure",
            ),
        ]
        vs = exposure_parent_materializations([exposure, parent], rels, default_config)
        assert len(vs) == 0
