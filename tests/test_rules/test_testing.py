"""Tests for testing rules."""

from dbt_linter.rules.testing import (
    check_test_coverage,
    missing_primary_key_tests,
    sources_without_freshness,
)


class TestMissingPrimaryKeyTests:
    def test_flags_model_without_pk_test(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model", is_primary_key_tested=False
        )
        assert (
            missing_primary_key_tests(r, default_config) is not None
        )

    def test_clean_model_with_pk_test(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model", is_primary_key_tested=True
        )
        assert missing_primary_key_tests(r, default_config) is None

    def test_ignores_sources(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source", is_primary_key_tested=False
        )
        assert missing_primary_key_tests(r, default_config) is None


class TestSourcesWithoutFreshness:
    def test_flags_source_without_freshness(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source", is_freshness_enabled=False
        )
        assert (
            sources_without_freshness(r, default_config) is not None
        )

    def test_clean_source_with_freshness(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="source", is_freshness_enabled=True
        )
        assert sources_without_freshness(r, default_config) is None

    def test_ignores_models(
        self, make_resource, default_config
    ):
        r = make_resource(
            resource_type="model", is_freshness_enabled=False
        )
        assert sources_without_freshness(r, default_config) is None


class TestTestCoverage:
    def test_flags_below_target(
        self, make_resource, default_config
    ):
        resources = [
            make_resource(
                resource_type="model",
                model_type="staging",
                is_primary_key_tested=True,
            ),
            make_resource(
                resource_type="model",
                model_type="staging",
                is_primary_key_tested=False,
            ),
        ]
        vs = check_test_coverage(resources, [], default_config)
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
                is_primary_key_tested=True,
            ),
        ]
        vs = check_test_coverage(resources, [], default_config)
        staging_vs = [
            v for v in vs if v.resource_name == "staging"
        ]
        assert len(staging_vs) == 0
