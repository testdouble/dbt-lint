"""Tests for structure rules."""

from dbt_linter.rules.structure import (
    intermediate_materialization,
    marts_materialization,
    model_directories,
    model_naming_conventions,
    source_directories,
    staging_materialization,
)


class TestModelNamingConventions:
    def test_flags_staging_without_prefix(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="orders",
        )
        assert (
            model_naming_conventions(r, default_config) is not None
        )

    def test_clean_staging_with_prefix(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="stg_orders",
        )
        assert model_naming_conventions(r, default_config) is None

    def test_clean_marts_with_fct_prefix(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            resource_name="fct_orders",
        )
        assert model_naming_conventions(r, default_config) is None

    def test_clean_marts_with_dim_prefix(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            resource_name="dim_customers",
        )
        assert model_naming_conventions(r, default_config) is None

    def test_ignores_sources(
        self, make_resource, default_config
    ):
        r = make_resource(resource_type="source", model_type="")
        assert model_naming_conventions(r, default_config) is None


class TestModelDirectories:
    def test_flags_staging_model_in_wrong_dir(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            file_path="models/marts/stg_orders.sql",
        )
        assert model_directories(r, default_config) is not None

    def test_clean_staging_in_staging_dir(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            file_path="models/staging/stg_orders.sql",
        )
        assert model_directories(r, default_config) is None


class TestSourceDirectories:
    def test_flags_source_not_in_staging(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source",
            file_path="models/marts/sources.yml",
        )
        assert source_directories(r, default_config) is not None

    def test_clean_source_in_staging(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source",
            file_path="models/staging/raw/sources.yml",
        )
        assert source_directories(r, default_config) is None

    def test_ignores_models(
        self, make_resource, default_config
    ):
        r = make_resource(resource_type="model")
        assert source_directories(r, default_config) is None


class TestStagingMaterialization:
    def test_flags_staging_table(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="table",
        )
        assert (
            staging_materialization(r, default_config) is not None
        )

    def test_clean_staging_view(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="view",
        )
        assert staging_materialization(r, default_config) is None


class TestIntermediateMaterialization:
    def test_flags_intermediate_table(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="intermediate",
            materialization="table",
        )
        assert (
            intermediate_materialization(r, default_config)
            is not None
        )

    def test_clean_intermediate_ephemeral(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="intermediate",
            materialization="ephemeral",
        )
        assert (
            intermediate_materialization(r, default_config) is None
        )


class TestMartsMaterialization:
    def test_flags_marts_view(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            materialization="view",
        )
        assert (
            marts_materialization(r, default_config) is not None
        )

    def test_clean_marts_table(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            materialization="table",
        )
        assert marts_materialization(r, default_config) is None
