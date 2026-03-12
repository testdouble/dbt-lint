"""Tests for documentation rules."""

from dbt_linter.models import ColumnInfo
from dbt_linter.rules.documentation import (
    column_documentation_coverage,
    documentation_coverage,
    undocumented_models,
    undocumented_source_tables,
    undocumented_sources,
)


class TestUndocumentedModels:
    def test_flags_undocumented_model(self, make_resource, default_config):
        r = make_resource(resource_type="model", is_described=False)
        assert undocumented_models(r, default_config) is not None

    def test_clean_documented_model(self, make_resource, default_config):
        r = make_resource(resource_type="model", is_described=True)
        assert undocumented_models(r, default_config) is None

    def test_ignores_sources(self, make_resource, default_config):
        r = make_resource(resource_type="source", is_described=False)
        assert undocumented_models(r, default_config) is None


class TestUndocumentedSources:
    def test_flags_source_without_source_description(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source",
            meta={"source_description_populated": False},
        )
        assert undocumented_sources(r, default_config) is not None

    def test_clean_source_with_description(self, make_resource, default_config):
        r = make_resource(
            resource_type="source",
            meta={"source_description_populated": True},
        )
        assert undocumented_sources(r, default_config) is None

    def test_ignores_models(self, make_resource, default_config):
        r = make_resource(resource_type="model")
        assert undocumented_sources(r, default_config) is None


class TestUndocumentedSourceTables:
    def test_flags_source_table_without_description(
        self, make_resource, default_config
    ):
        r = make_resource(resource_type="source", is_described=False)
        v = undocumented_source_tables(r, default_config)
        assert v is not None

    def test_clean_source_table_with_description(self, make_resource, default_config):
        r = make_resource(resource_type="source", is_described=True)
        assert undocumented_source_tables(r, default_config) is None


class TestDocumentationCoverage:
    def test_flags_below_target(self, make_resource, default_config):
        resources = [
            make_resource(
                resource_type="model",
                model_type="staging",
                is_described=True,
            ),
            make_resource(
                resource_type="model",
                model_type="staging",
                is_described=False,
            ),
        ]
        vs = documentation_coverage(resources, [], default_config)
        staging_vs = [v for v in vs if v.resource_name == "staging"]
        assert len(staging_vs) == 1
        assert "50%" in staging_vs[0].message

    def test_clean_at_target(self, make_resource, default_config):
        resources = [
            make_resource(
                resource_type="model",
                model_type="staging",
                is_described=True,
            ),
        ]
        vs = documentation_coverage(resources, [], default_config)
        staging_vs = [v for v in vs if v.resource_name == "staging"]
        assert len(staging_vs) == 0


class TestColumnDocumentationCoverage:
    """Column-level doc coverage: disabled by default, config-driven."""

    def test_null_config_returns_empty(self, make_resource, default_config):
        r = make_resource(
            columns=(ColumnInfo(name="id", data_type="integer", is_described=False),),
        )
        result = column_documentation_coverage([r], [], default_config)
        assert result == []

    def test_zero_coverage_flagged(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 100
        r = make_resource(
            columns=(
                ColumnInfo(name="id", data_type="integer", is_described=False),
                ColumnInfo(name="name", data_type="text", is_described=False),
            ),
        )
        result = column_documentation_coverage([r], [], default_config)
        assert len(result) == 1
        assert "0%" in result[0].message
        assert "100%" in result[0].message

    def test_partial_coverage_flagged(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 100
        r = make_resource(
            columns=(
                ColumnInfo(name="id", data_type="integer", is_described=True),
                ColumnInfo(name="name", data_type="text", is_described=False),
            ),
        )
        result = column_documentation_coverage([r], [], default_config)
        assert len(result) == 1
        assert "50%" in result[0].message

    def test_full_coverage_clean(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 100
        r = make_resource(
            columns=(
                ColumnInfo(name="id", data_type="integer", is_described=True),
                ColumnInfo(name="name", data_type="text", is_described=True),
            ),
        )
        result = column_documentation_coverage([r], [], default_config)
        assert result == []

    def test_partial_coverage_below_custom_target(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 80
        r = make_resource(
            columns=(
                ColumnInfo(name="a", data_type="", is_described=True),
                ColumnInfo(name="b", data_type="", is_described=False),
                ColumnInfo(name="c", data_type="", is_described=False),
                ColumnInfo(name="d", data_type="", is_described=False),
            ),
        )
        result = column_documentation_coverage([r], [], default_config)
        assert len(result) == 1
        assert "25%" in result[0].message

    def test_no_columns_declared_skipped(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 100
        r = make_resource(columns=())
        result = column_documentation_coverage([r], [], default_config)
        assert result == []

    def test_sources_skipped(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 100
        r = make_resource(
            resource_type="source",
            columns=(ColumnInfo(name="id", data_type="integer", is_described=False),),
        )
        result = column_documentation_coverage([r], [], default_config)
        assert result == []

    def test_exposures_skipped(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 100
        r = make_resource(resource_type="exposure")
        result = column_documentation_coverage([r], [], default_config)
        assert result == []

    def test_violation_uses_from_resource(self, make_resource, default_config):
        default_config.params["column_documentation_coverage_target"] = 100
        r = make_resource(
            columns=(ColumnInfo(name="id", data_type="integer", is_described=False),),
        )
        result = column_documentation_coverage([r], [], default_config)
        assert result[0].rule_id == ""
        assert result[0].severity == ""
