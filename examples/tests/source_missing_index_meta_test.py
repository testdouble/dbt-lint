"""Tests for source_missing_index_meta custom rule."""

from source_missing_index_meta import source_missing_index_meta


class TestSourceMissingIndexMeta:
    def test_flags_ft_source_without_meta(self, make_resource, default_context):
        r = make_resource(
            resource_type="source",
            resource_name="ft_something.patients",
            meta={},
        )
        v = source_missing_index_meta(r, default_context)
        assert v is not None
        assert "indexes_reviewed" in v.message

    def test_clean_ft_source_with_meta(self, make_resource, default_context):
        r = make_resource(
            resource_type="source",
            resource_name="ft_something.patients",
            meta={"indexes_reviewed": True},
        )
        assert source_missing_index_meta(r, default_context) is None

    def test_ignores_non_ft_source(self, make_resource, default_context):
        r = make_resource(
            resource_type="source",
            resource_name="stripe.charges",
            meta={},
        )
        assert source_missing_index_meta(r, default_context) is None

    def test_ignores_models(self, make_resource, default_context):
        r = make_resource(
            resource_type="model",
            resource_name="ft_something",
            meta={},
        )
        assert source_missing_index_meta(r, default_context) is None
