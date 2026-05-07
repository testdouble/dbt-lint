"""Tests for testing rules."""

from dbt_lint.rules.testing import (
    check_test_coverage,
    missing_primary_key_tests,
    missing_relationship_tests,
    sources_without_freshness,
    untested_models,
)


class TestMissingPrimaryKeyTests:
    def test_flags_model_without_pk_test(self, make_resource, default_context):
        resource = make_resource(resource_type="model", is_primary_key_tested=False)

        violation = missing_primary_key_tests(resource, default_context)

        assert violation is not None
        assert "missing primary key test" in violation.message

    def test_clean_model_with_pk_test(self, make_resource, default_context):
        resource = make_resource(resource_type="model", is_primary_key_tested=True)

        assert missing_primary_key_tests(resource, default_context) is None

    def test_ignores_sources(self, make_resource, default_context):
        resource = make_resource(resource_type="source", is_primary_key_tested=False)

        assert missing_primary_key_tests(resource, default_context) is None


class TestSourcesWithoutFreshness:
    def test_flags_source_without_freshness(self, make_resource, default_context):
        resource = make_resource(resource_type="source", is_freshness_enabled=False)

        violation = sources_without_freshness(resource, default_context)

        assert violation is not None
        assert "no freshness check configured" in violation.message

    def test_clean_source_with_freshness(self, make_resource, default_context):
        resource = make_resource(resource_type="source", is_freshness_enabled=True)

        assert sources_without_freshness(resource, default_context) is None

    def test_ignores_models(self, make_resource, default_context):
        resource = make_resource(resource_type="model", is_freshness_enabled=False)

        assert sources_without_freshness(resource, default_context) is None


class TestTestCoverage:
    def test_flags_below_target(self, make_resource, default_context):
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

        violations = check_test_coverage(resources, [], default_context)
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
                is_primary_key_tested=True,
            ),
        ]

        violations = check_test_coverage(resources, [], default_context)
        staging_vs = [
            violation
            for violation in violations
            if violation.resource_name == "staging"
        ]

        assert len(staging_vs) == 0


class TestMissingRelationshipTests:
    def test_flags_model_with_refs_but_no_relationship_tests(
        self, make_resource, make_relationship, default_context
    ):
        """A model that refs other models but has no relationship tests."""
        child = make_resource(
            resource_id="model.pkg.orders",
            resource_type="model",
            model_type="marts",
            has_relationship_tests=False,
        )
        rels = [
            make_relationship(
                parent="model.pkg.customers",
                child="model.pkg.orders",
                parent_resource_type="model",
                child_resource_type="model",
                distance=1,
            ),
        ]

        violations = missing_relationship_tests([child], rels, default_context)

        assert len(violations) == 1

    def test_clean_model_with_relationship_tests(
        self, make_resource, make_relationship, default_context
    ):
        child = make_resource(
            resource_id="model.pkg.orders",
            resource_type="model",
            model_type="marts",
            has_relationship_tests=True,
        )
        rels = [
            make_relationship(
                parent="model.pkg.customers",
                child="model.pkg.orders",
                parent_resource_type="model",
                child_resource_type="model",
                distance=1,
            ),
        ]

        violations = missing_relationship_tests([child], rels, default_context)

        assert not violations

    def test_ignores_staging_models(
        self, make_resource, make_relationship, default_context
    ):
        child = make_resource(
            resource_id="model.pkg.stg_orders",
            resource_type="model",
            model_type="staging",
            has_relationship_tests=False,
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.orders",
                child="model.pkg.stg_orders",
                parent_resource_type="source",
                child_resource_type="model",
                distance=1,
            ),
        ]

        violations = missing_relationship_tests([child], rels, default_context)

        assert not violations

    def test_ignores_models_with_no_model_parents(self, make_resource, default_context):
        child = make_resource(
            resource_id="model.pkg.stg_orders",
            resource_type="model",
            model_type="other",
            has_relationship_tests=False,
        )

        violations = missing_relationship_tests([child], [], default_context)

        assert not violations


class TestUntestedModels:
    def test_flags_model_with_zero_tests(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            number_of_tests=0,
        )

        result = untested_models(resource, default_context)

        assert "no tests" in result.message

    def test_clean_model_with_tests(self, make_resource, default_context):
        resource = make_resource(
            resource_type="model",
            number_of_tests=3,
        )

        assert untested_models(resource, default_context) is None

    def test_skips_sources(self, make_resource, default_context):
        resource = make_resource(
            resource_type="source",
            number_of_tests=0,
        )

        assert untested_models(resource, default_context) is None

    def test_skips_exposures(self, make_resource, default_context):
        resource = make_resource(
            resource_type="exposure",
            number_of_tests=0,
        )

        assert untested_models(resource, default_context) is None
