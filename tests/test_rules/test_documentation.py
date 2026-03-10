"""Tests for documentation rules."""

from dbt_linter.rules.documentation import (
    documentation_coverage,
    undocumented_models,
    undocumented_source_tables,
    undocumented_sources,
)


class TestUndocumentedModels:
    def test_flags_undocumented_model(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model", is_described=False
        )
        assert undocumented_models(r, default_config) is not None

    def test_clean_documented_model(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model", is_described=True
        )
        assert undocumented_models(r, default_config) is None

    def test_ignores_sources(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source", is_described=False
        )
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

    def test_clean_source_with_description(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source",
            meta={"source_description_populated": True},
        )
        assert undocumented_sources(r, default_config) is None

    def test_ignores_models(
        self, make_resource, default_config
    ):
        r = make_resource(resource_type="model")
        assert undocumented_sources(r, default_config) is None


class TestUndocumentedSourceTables:
    def test_flags_source_table_without_description(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source", is_described=False
        )
        v = undocumented_source_tables(r, default_config)
        assert v is not None

    def test_clean_source_table_with_description(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source", is_described=True
        )
        assert undocumented_source_tables(r, default_config) is None


class TestDocumentationCoverage:
    def test_flags_below_target(
        self, make_resource, default_config
    ):
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
        vs = documentation_coverage(
            resources, [], default_config
        )
        staging_vs = [
            v for v in vs if v.resource_name == "staging"
        ]
        assert len(staging_vs) == 1
        assert "50%" in staging_vs[0].message

    def test_clean_at_target(
        self, make_resource, default_config
    ):
        resources = [
            make_resource(
                resource_type="model",
                model_type="staging",
                is_described=True,
            ),
        ]
        vs = documentation_coverage(
            resources, [], default_config
        )
        staging_vs = [
            v for v in vs if v.resource_name == "staging"
        ]
        assert len(staging_vs) == 0
