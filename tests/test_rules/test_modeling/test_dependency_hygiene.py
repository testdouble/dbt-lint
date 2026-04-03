"""Tests for modeling dependency hygiene rules."""

from dbt_linter.rules.modeling import (
    duplicate_sources,
    hard_coded_references,
    multiple_sources_joined,
    unused_sources,
)


class TestHardCodedReferences:
    def test_flags_hard_coded(self, make_resource, default_config):
        resource = make_resource(resource_type="model", hard_coded_references=True)

        violation = hard_coded_references(resource, default_config)

        assert violation is not None
        assert "hard-coded table references" in violation.message

    def test_clean(self, make_resource, default_config):
        resource = make_resource(resource_type="model", hard_coded_references=False)

        assert hard_coded_references(resource, default_config) is None

    def test_ignores_non_models(self, make_resource, default_config):
        resource = make_resource(resource_type="source", hard_coded_references=True)

        assert hard_coded_references(resource, default_config) is None


class TestDuplicateSources:
    def test_flags_duplicates(self, make_resource, default_config):
        s1 = make_resource(
            resource_id="source.a.raw.orders",
            resource_type="source",
            resource_name="orders",
            database="db",
            schema_name="raw",
        )
        s2 = make_resource(
            resource_id="source.b.raw.orders",
            resource_type="source",
            resource_name="orders",
            database="db",
            schema_name="raw",
        )

        violations = duplicate_sources([s1, s2], [], default_config)

        assert len(violations) == 1

    def test_clean_when_different_tables(self, make_resource, default_config):
        s1 = make_resource(
            resource_type="source",
            resource_name="orders",
            database="db",
            schema_name="raw",
        )
        s2 = make_resource(
            resource_type="source",
            resource_name="customers",
            database="db",
            schema_name="raw",
        )

        assert not duplicate_sources([s1, s2], [], default_config)


class TestUnusedSources:
    def test_flags_source_with_no_children(self, make_resource, default_config):
        source = make_resource(
            resource_id="source.pkg.raw.t",
            resource_type="source",
        )

        violations = unused_sources([source], [], default_config)

        assert len(violations) == 1

    def test_clean_when_source_has_child(
        self, make_resource, make_relationship, default_config
    ):
        source = make_resource(
            resource_id="source.pkg.raw.t",
            resource_type="source",
        )
        rels = [
            make_relationship(
                parent="source.pkg.raw.t",
                child="model.pkg.stg",
                parent_resource_type="source",
            ),
        ]

        assert not unused_sources([source], rels, default_config)


class TestMultipleSourcesJoined:
    def test_flags_model_with_two_source_parents(
        self, make_resource, make_relationship, default_config
    ):
        model = make_resource(resource_id="model.pkg.m")
        rels = [
            make_relationship(
                parent="source.pkg.a",
                child="model.pkg.m",
                parent_resource_type="source",
                child_resource_type="model",
            ),
            make_relationship(
                parent="source.pkg.b",
                child="model.pkg.m",
                parent_resource_type="source",
                child_resource_type="model",
            ),
        ]

        violations = multiple_sources_joined([model], rels, default_config)

        assert len(violations) == 1
