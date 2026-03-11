"""Tests for structure rules."""

from dbt_linter.rules.structure import (
    intermediate_materialization,
    marts_materialization,
    model_directories,
    model_name_format,
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

    def test_clean_marts_plain_name_with_empty_default(
        self, make_resource, default_config
    ):
        """With empty marts_prefixes (default), any name passes."""
        r = make_resource(
            resource_type="model",
            model_type="marts",
            resource_name="orders",
        )
        assert model_naming_conventions(r, default_config) is None

    def test_flags_marts_without_prefix_when_overridden(
        self, make_resource, default_config
    ):
        """With explicit marts_prefixes, plain name is flagged."""
        default_config.params["marts_prefixes"] = ["fct_", "dim_"]
        r = make_resource(
            resource_type="model",
            model_type="marts",
            resource_name="orders",
        )
        assert model_naming_conventions(r, default_config) is not None


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

    def test_folder_name_as_list(self, make_resource, default_config):
        """When folder_name is a list, any matching dir passes."""
        default_config.params["intermediate_folder_name"] = [
            "intermediate",
            "transformed_intermediate",
        ]
        r = make_resource(
            resource_type="model",
            model_type="intermediate",
            file_path="models/transformed_intermediate/int_orders.sql",
        )
        assert model_directories(r, default_config) is None

    def test_folder_name_as_list_flags_mismatch(
        self, make_resource, default_config
    ):
        default_config.params["intermediate_folder_name"] = [
            "intermediate",
            "transformed_intermediate",
        ]
        r = make_resource(
            resource_type="model",
            model_type="intermediate",
            file_path="models/reporting/int_orders.sql",
        )
        assert model_directories(r, default_config) is not None


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


class TestModelNameFormat:
    def test_clean_snake_case(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            resource_name="stg_orders",
        )
        assert model_name_format(r, default_config) is None

    def test_clean_with_numbers(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            resource_name="stg_orders_v2",
        )
        assert model_name_format(r, default_config) is None

    def test_flags_uppercase(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            resource_name="Stg_Orders",
        )
        v = model_name_format(r, default_config)
        assert v is not None
        assert "snake_case" in v.message

    def test_flags_dots(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            resource_name="stg.orders",
        )
        assert model_name_format(r, default_config) is not None

    def test_flags_hyphens(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            resource_name="stg-orders",
        )
        assert model_name_format(r, default_config) is not None

    def test_flags_leading_number(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            resource_name="1_orders",
        )
        assert model_name_format(r, default_config) is not None

    def test_ignores_sources(self, make_resource, default_config):
        r = make_resource(
            resource_type="source",
            resource_name="RawOrders",
        )
        assert model_name_format(r, default_config) is None

    def test_ignores_exposures(self, make_resource, default_config):
        r = make_resource(
            resource_type="exposure",
            resource_name="Weekly-Dashboard",
        )
        assert model_name_format(r, default_config) is None
