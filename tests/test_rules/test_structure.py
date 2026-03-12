"""Tests for structure rules."""

from dbt_linter.models import ColumnInfo
from dbt_linter.rules.structure import (
    column_naming_conventions,
    intermediate_materialization,
    marts_materialization,
    model_directories,
    model_name_format,
    model_naming_conventions,
    source_directories,
    staging_materialization,
    staging_naming_convention,
    yaml_file_naming,
)


class TestModelNamingConventions:
    def test_flags_staging_without_prefix(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="orders",
        )
        assert model_naming_conventions(r, default_config) is not None

    def test_clean_staging_with_prefix(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="stg_orders",
        )
        assert model_naming_conventions(r, default_config) is None

    def test_clean_marts_with_fct_prefix(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            resource_name="fct_orders",
        )
        assert model_naming_conventions(r, default_config) is None

    def test_clean_marts_with_dim_prefix(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            resource_name="dim_customers",
        )
        assert model_naming_conventions(r, default_config) is None

    def test_ignores_sources(self, make_resource, default_config):
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
    def test_flags_staging_model_in_wrong_dir(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            file_path="models/marts/stg_orders.sql",
        )
        assert model_directories(r, default_config) is not None

    def test_clean_staging_in_staging_dir(self, make_resource, default_config):
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

    def test_folder_name_as_list_flags_mismatch(self, make_resource, default_config):
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
    def test_flags_source_not_in_staging(self, make_resource, default_config):
        r = make_resource(
            resource_type="source",
            file_path="models/marts/sources.yml",
        )
        assert source_directories(r, default_config) is not None

    def test_clean_source_in_staging(self, make_resource, default_config):
        r = make_resource(
            resource_type="source",
            file_path="models/staging/raw/sources.yml",
        )
        assert source_directories(r, default_config) is None

    def test_ignores_models(self, make_resource, default_config):
        r = make_resource(resource_type="model")
        assert source_directories(r, default_config) is None


class TestStagingMaterialization:
    def test_flags_staging_table(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="table",
        )
        assert staging_materialization(r, default_config) is not None

    def test_clean_staging_view(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="view",
        )
        assert staging_materialization(r, default_config) is None


class TestIntermediateMaterialization:
    def test_flags_intermediate_table(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="intermediate",
            materialization="table",
        )
        assert intermediate_materialization(r, default_config) is not None

    def test_clean_intermediate_ephemeral(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="intermediate",
            materialization="ephemeral",
        )
        assert intermediate_materialization(r, default_config) is None


class TestMartsMaterialization:
    def test_flags_marts_view(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            materialization="view",
        )
        assert marts_materialization(r, default_config) is not None

    def test_clean_marts_table(self, make_resource, default_config):
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


class TestStagingNamingConvention:
    def test_flags_staging_without_double_underscore(
        self, make_resource, default_config
    ):
        """stg_orders is missing the source__entity pattern."""
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="stg_orders",
        )
        v = staging_naming_convention(r, default_config)
        assert v is not None
        assert "__" in v.message

    def test_clean_staging_with_double_underscore(self, make_resource, default_config):
        """stg_stripe__orders follows the convention."""
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="stg_stripe__orders",
        )
        assert staging_naming_convention(r, default_config) is None

    def test_clean_staging_with_nested_underscores(self, make_resource, default_config):
        """stg_google_analytics__page_views is valid."""
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="stg_google_analytics__page_views",
        )
        assert staging_naming_convention(r, default_config) is None

    def test_ignores_non_staging(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            resource_name="orders",
        )
        assert staging_naming_convention(r, default_config) is None

    def test_ignores_sources(self, make_resource, default_config):
        r = make_resource(resource_type="source")
        assert staging_naming_convention(r, default_config) is None

    def test_ignores_when_no_staging_prefix_matched(
        self, make_resource, default_config
    ):
        """If name doesn't start with any staging prefix, skip
        (model-naming-conventions handles that)."""
        r = make_resource(
            resource_type="model",
            model_type="staging",
            resource_name="orders",
        )
        assert staging_naming_convention(r, default_config) is None


class TestYamlFileNaming:
    def test_flags_source_yaml_without_leading_underscore(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source",
            file_path="models/staging/stripe/sources.yml",
        )
        v = yaml_file_naming(r, default_config)
        assert v is not None

    def test_clean_source_yaml_with_convention(self, make_resource, default_config):
        r = make_resource(
            resource_type="source",
            file_path="models/staging/stripe/_stripe__sources.yml",
        )
        assert yaml_file_naming(r, default_config) is None

    def test_flags_model_yaml_without_convention(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            patch_path="project://models/staging/stripe/models.yml",
        )
        v = yaml_file_naming(r, default_config)
        assert v is not None

    def test_clean_model_yaml_with_convention(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            patch_path="project://models/staging/stripe/_stripe__models.yml",
        )
        assert yaml_file_naming(r, default_config) is None

    def test_ignores_model_without_patch_path(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            patch_path="",
        )
        assert yaml_file_naming(r, default_config) is None

    def test_ignores_exposures(self, make_resource, default_config):
        r = make_resource(resource_type="exposure")
        assert yaml_file_naming(r, default_config) is None


class TestColumnNamingConventions:
    """Column naming conventions: disabled by default, config-driven."""

    def test_null_config_returns_empty(self, make_resource, default_config):
        """No column_naming_conventions config -> no violations."""
        r = make_resource(
            columns=(
                ColumnInfo(name="order_count", data_type="integer", is_described=True),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert result == []

    def test_forbidden_suffix_flagged(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "forbidden_suffixes": {"_count": "_cnt"},
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="order_count",
                    data_type="integer",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert len(result) == 1
        assert "_count" in result[0].message
        assert "_cnt" in result[0].message

    def test_forbidden_suffix_clean(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "forbidden_suffixes": {"_count": "_cnt"},
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="order_cnt",
                    data_type="integer",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert result == []

    def test_boolean_prefix_flagged(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "boolean_prefixes": ["is_", "has_"],
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="active",
                    data_type="boolean",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert len(result) == 1
        assert "boolean" in result[0].message

    def test_boolean_prefix_clean(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "boolean_prefixes": ["is_", "has_"],
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="is_active",
                    data_type="boolean",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert result == []

    def test_boolean_prefix_ignores_non_boolean(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "boolean_prefixes": ["is_", "has_"],
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="amount",
                    data_type="numeric",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert result == []

    def test_type_suffix_flagged(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "type_suffixes": {"timestamp": "_at"},
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="created",
                    data_type="timestamp",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert len(result) == 1
        assert "_at" in result[0].message

    def test_type_suffix_clean(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "type_suffixes": {"timestamp": "_at"},
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="created_at",
                    data_type="timestamp",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert result == []

    def test_multiple_violations_same_resource(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "forbidden_suffixes": {"_count": "_cnt"},
            "type_suffixes": {"timestamp": "_at"},
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="order_count",
                    data_type="integer",
                    is_described=True,
                ),
                ColumnInfo(
                    name="created",
                    data_type="timestamp",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert len(result) == 2

    def test_ignores_sources(self, make_resource, default_config):
        default_config.params["column_naming_conventions"] = {
            "forbidden_suffixes": {"_count": "_cnt"},
        }
        r = make_resource(
            resource_type="source",
            columns=(
                ColumnInfo(
                    name="order_count",
                    data_type="integer",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert result == []

    def test_violation_uses_from_resource(self, make_resource, default_config):
        """Violations use Violation.from_resource (rule_id/severity empty)."""
        default_config.params["column_naming_conventions"] = {
            "forbidden_suffixes": {"_count": "_cnt"},
        }
        r = make_resource(
            columns=(
                ColumnInfo(
                    name="order_count",
                    data_type="integer",
                    is_described=True,
                ),
            ),
        )
        result = column_naming_conventions([r], [], default_config)
        assert result[0].rule_id == ""
        assert result[0].severity == ""
