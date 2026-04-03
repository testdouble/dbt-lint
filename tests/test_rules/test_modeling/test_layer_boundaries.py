"""Tests for modeling layer boundary rules."""

from dbt_linter.rules.modeling import (
    direct_join_to_source,
    downstream_depends_on_source,
    root_models,
    staging_depends_on_downstream,
    staging_depends_on_staging,
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

        violations = direct_join_to_source([child], rels, default_config)

        assert len(violations) == 1

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

        assert not direct_join_to_source([child], rels, default_config)


class TestDownstreamDependsOnSource:
    def test_flags_marts_depending_on_source(
        self, make_resource, make_relationship, default_config
    ):
        model = make_resource(resource_id="model.pkg.fct")
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.fct",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="marts",
            ),
        ]

        violations = downstream_depends_on_source([model], rels, default_config)

        assert len(violations) == 1

    def test_clean_for_staging(self, make_resource, make_relationship, default_config):
        model = make_resource(resource_id="model.pkg.stg")
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.stg",
                parent_resource_type="source",
                child_resource_type="model",
                child_model_type="staging",
            ),
        ]

        assert not downstream_depends_on_source([model], rels, default_config)


class TestStagingDependsOnStaging:
    def test_flags_staging_to_staging(self, make_relationship, default_config):
        rels = [
            make_relationship(
                parent="model.pkg.stg_a",
                child="model.pkg.stg_b",
                parent_model_type="staging",
                child_model_type="staging",
            ),
        ]

        violations = staging_depends_on_staging([], rels, default_config)

        assert len(violations) == 1


class TestStagingDependsOnDownstream:
    def test_flags_staging_depending_on_marts(self, make_relationship, default_config):
        rels = [
            make_relationship(
                parent="model.pkg.fct_x",
                child="model.pkg.stg_y",
                parent_model_type="marts",
                child_model_type="staging",
            ),
        ]

        violations = staging_depends_on_downstream([], rels, default_config)

        assert len(violations) == 1


class TestRootModels:
    def test_flags_model_with_no_parents(
        self, make_resource, make_relationship, default_config
    ):
        orphan = make_resource(
            resource_id="model.pkg.orphan",
            resource_type="model",
        )

        violations = root_models([orphan], [], default_config)

        assert len(violations) == 1

    def test_clean_when_model_has_parent(
        self, make_resource, make_relationship, default_config
    ):
        model = make_resource(resource_id="model.pkg.m", resource_type="model")
        rels = [
            make_relationship(
                parent="source.pkg.s",
                child="model.pkg.m",
                child_resource_type="model",
            ),
        ]

        violations = root_models([model], rels, default_config)

        assert len(violations) == 0
