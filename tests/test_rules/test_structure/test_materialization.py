"""Tests for structure materialization rules."""

from dbt_lint.rules.structure import (
    intermediate_materialization,
    marts_materialization,
    staging_materialization,
)


class TestStagingMaterialization:
    def test_flags_staging_table(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="table",
        )

        violation = staging_materialization(resource, default_config)

        assert violation is not None
        assert "table not allowed for staging" in violation.message

    def test_clean_staging_view(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="view",
        )

        assert staging_materialization(resource, default_config) is None


class TestIntermediateMaterialization:
    def test_flags_intermediate_table(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            model_type="intermediate",
            materialization="table",
        )

        violation = intermediate_materialization(resource, default_config)

        assert violation is not None
        assert "table not allowed for intermediate" in violation.message

    def test_clean_intermediate_ephemeral(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            model_type="intermediate",
            materialization="ephemeral",
        )

        assert intermediate_materialization(resource, default_config) is None


class TestMartsMaterialization:
    def test_flags_marts_view(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            model_type="marts",
            materialization="view",
        )

        violation = marts_materialization(resource, default_config)

        assert violation is not None
        assert "view not allowed for marts" in violation.message

    def test_clean_marts_table(self, make_resource, default_config):
        resource = make_resource(
            resource_type="model",
            model_type="marts",
            materialization="table",
        )

        assert marts_materialization(resource, default_config) is None
