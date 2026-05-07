"""Tests for documentation rules."""

from dbt_lint.models import ColumnInfo
from dbt_lint.rules.documentation import (
    column_documentation_coverage,
    documentation_coverage,
    undocumented_models,
    undocumented_source_tables,
    undocumented_sources,
)


class TestUndocumentedModels:
    def test_flags_undocumented_model(self, make_resource, default_context):
        resource = make_resource(resource_type="model", is_described=False)

        violation = undocumented_models(resource, default_context)

        assert violation is not None
        assert "missing description" in violation.message

    def test_clean_documented_model(self, make_resource, default_context):
        resource = make_resource(resource_type="model", is_described=True)

        assert undocumented_models(resource, default_context) is None

    def test_ignores_sources(self, make_resource, default_context):
        resource = make_resource(resource_type="source", is_described=False)

        assert undocumented_models(resource, default_context) is None


class TestUndocumentedSources:
    def test_flags_source_without_source_description(
        self, make_resource, default_context
    ):
        resource = make_resource(
            resource_type="source",
            meta={"source_description_populated": False},
        )

        violation = undocumented_sources(resource, default_context)

        assert violation is not None
        assert "source missing description" in violation.message

    def test_clean_source_with_description(self, make_resource, default_context):
        resource = make_resource(
            resource_type="source",
            meta={"source_description_populated": True},
        )

        assert undocumented_sources(resource, default_context) is None

    def test_ignores_models(self, make_resource, default_context):
        resource = make_resource(resource_type="model")

        assert undocumented_sources(resource, default_context) is None


class TestUndocumentedSourceTables:
    def test_flags_source_table_without_description(
        self, make_resource, default_context
    ):
        resource = make_resource(resource_type="source", is_described=False)

        violation = undocumented_source_tables(resource, default_context)

        assert violation is not None
        assert "source table missing description" in violation.message

    def test_clean_source_table_with_description(self, make_resource, default_context):
        resource = make_resource(resource_type="source", is_described=True)

        assert undocumented_source_tables(resource, default_context) is None


class TestDocumentationCoverage:
    def test_flags_below_target(self, make_resource, default_context):
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

        violations = documentation_coverage(resources, [], default_context)
        staging_vs = [
            violation
            for violation in violations
            if violation.resource_name == "staging"
        ]

        assert len(staging_vs) == 1
        assert "50%" in staging_vs[0].message

    def test_clean_at_target(self, make_resource, default_context):
        resources = [
            make_resource(
                resource_type="model",
                model_type="staging",
                is_described=True,
            ),
        ]

        violations = documentation_coverage(resources, [], default_context)
        staging_vs = [
            violation
            for violation in violations
            if violation.resource_name == "staging"
        ]

        assert len(staging_vs) == 0


class TestColumnDocumentationCoverage:
    """Column-level doc coverage: disabled by default, config-driven."""

    def test_null_config_returns_empty(self, make_resource, default_context):
        resource = make_resource(
            columns=(ColumnInfo(name="id", data_type="integer", is_described=False),),
        )

        result = column_documentation_coverage([resource], [], default_context)

        assert not result

    def test_zero_coverage_flagged(self, make_resource, default_context):
        default_context.params["column_documentation_coverage_target"] = 100
        resource = make_resource(
            columns=(
                ColumnInfo(name="id", data_type="integer", is_described=False),
                ColumnInfo(name="name", data_type="text", is_described=False),
            ),
        )

        result = column_documentation_coverage([resource], [], default_context)

        assert len(result) == 1
        assert "0%" in result[0].message
        assert "100%" in result[0].message

    def test_partial_coverage_flagged(self, make_resource, default_context):
        default_context.params["column_documentation_coverage_target"] = 100
        resource = make_resource(
            columns=(
                ColumnInfo(name="id", data_type="integer", is_described=True),
                ColumnInfo(name="name", data_type="text", is_described=False),
            ),
        )

        result = column_documentation_coverage([resource], [], default_context)

        assert len(result) == 1
        assert "50%" in result[0].message

    def test_full_coverage_clean(self, make_resource, default_context):
        default_context.params["column_documentation_coverage_target"] = 100
        resource = make_resource(
            columns=(
                ColumnInfo(name="id", data_type="integer", is_described=True),
                ColumnInfo(name="name", data_type="text", is_described=True),
            ),
        )

        result = column_documentation_coverage([resource], [], default_context)

        assert not result

    def test_partial_coverage_below_custom_target(self, make_resource, default_context):
        default_context.params["column_documentation_coverage_target"] = 80
        resource = make_resource(
            columns=(
                ColumnInfo(name="a", data_type="", is_described=True),
                ColumnInfo(name="b", data_type="", is_described=False),
                ColumnInfo(name="c", data_type="", is_described=False),
                ColumnInfo(name="d", data_type="", is_described=False),
            ),
        )

        result = column_documentation_coverage([resource], [], default_context)

        assert len(result) == 1
        assert "25%" in result[0].message

    def test_no_columns_declared_skipped(self, make_resource, default_context):
        default_context.params["column_documentation_coverage_target"] = 100
        resource = make_resource(columns=())

        result = column_documentation_coverage([resource], [], default_context)

        assert not result

    def test_sources_skipped(self, make_resource, default_context):
        default_context.params["column_documentation_coverage_target"] = 100
        resource = make_resource(
            resource_type="source",
            columns=(ColumnInfo(name="id", data_type="integer", is_described=False),),
        )

        result = column_documentation_coverage([resource], [], default_context)

        assert not result

    def test_exposures_skipped(self, make_resource, default_context):
        default_context.params["column_documentation_coverage_target"] = 100
        resource = make_resource(resource_type="exposure")

        result = column_documentation_coverage([resource], [], default_context)

        assert not result
