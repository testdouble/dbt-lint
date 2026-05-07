"""Tests for performance rules."""

from dbt_lint.rules.performance import (
    chained_views,
    exposure_parent_materializations,
    incremental_missing_unique_key,
)


class TestChainedViews:
    def test_flags_chain_exceeding_threshold(
        self, make_resource, make_relationship, default_context
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

        violations = chained_views([child], rels, default_context)

        assert len(violations) == 1
        assert "depth 6" in violations[0].message

    def test_clean_below_threshold(
        self, make_resource, make_relationship, default_context
    ):
        child = make_resource(resource_id="model.pkg.deep")
        rels = [
            make_relationship(
                parent="model.pkg.root",
                child="model.pkg.deep",
                distance=3,
                is_dependent_on_chain_of_views=True,
            ),
        ]

        assert not chained_views([child], rels, default_context)

    def test_clean_non_view_chain(
        self, make_resource, make_relationship, default_context
    ):
        child = make_resource(resource_id="model.pkg.deep")
        rels = [
            make_relationship(
                parent="model.pkg.root",
                child="model.pkg.deep",
                distance=6,
                is_dependent_on_chain_of_views=False,
            ),
        ]

        assert not chained_views([child], rels, default_context)


class TestExposureParentMaterializations:
    def test_flags_exposure_depending_on_view(
        self, make_resource, make_relationship, default_context
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

        violations = exposure_parent_materializations(
            [exposure, parent], rels, default_context
        )

        assert len(violations) == 1

    def test_flags_exposure_depending_on_source(
        self, make_resource, make_relationship, default_context
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

        violations = exposure_parent_materializations(
            [exposure, src], rels, default_context
        )

        assert len(violations) == 1

    def test_clean_exposure_depending_on_table(
        self, make_resource, make_relationship, default_context
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

        violations = exposure_parent_materializations(
            [exposure, parent], rels, default_context
        )

        assert len(violations) == 0


class TestIncrementalMissingUniqueKey:
    def test_flags_incremental_without_unique_key(self, make_resource, default_context):
        resource = make_resource(
            materialization="incremental",
            config={},
        )

        result = incremental_missing_unique_key(resource, default_context)

        assert result is not None
        assert "unique_key" in result.message

    def test_clean_incremental_with_unique_key(self, make_resource, default_context):
        resource = make_resource(
            materialization="incremental",
            config={"unique_key": "id"},
        )

        assert incremental_missing_unique_key(resource, default_context) is None

    def test_clean_incremental_with_unique_key_list(
        self, make_resource, default_context
    ):
        resource = make_resource(
            materialization="incremental",
            config={"unique_key": ["id", "date"]},
        )

        assert incremental_missing_unique_key(resource, default_context) is None

    def test_skips_non_incremental_models(self, make_resource, default_context):
        resource = make_resource(
            materialization="table",
            config={},
        )

        assert incremental_missing_unique_key(resource, default_context) is None

    def test_skips_non_model_resources(self, make_resource, default_context):
        resource = make_resource(
            resource_type="source",
            materialization="",
            config={},
        )

        assert incremental_missing_unique_key(resource, default_context) is None
