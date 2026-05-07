"""Tests for model_requires_scheduling_tag custom rule."""

from model_requires_scheduling_tag import model_requires_scheduling_tag


class TestModelRequiresSchedulingTag:
    def test_flags_model_without_scheduling_tag(self, make_resource, default_context):
        r = make_resource(
            resource_type="model",
            tags=("some_other_tag",),
        )
        v = model_requires_scheduling_tag(r, default_context)
        assert v is not None
        assert "scheduling tag" in v.message

    def test_flags_model_with_no_tags(self, make_resource, default_context):
        r = make_resource(resource_type="model", tags=())
        assert model_requires_scheduling_tag(r, default_context) is not None

    def test_clean_model_with_scheduled_update_tag(
        self, make_resource, default_context
    ):
        r = make_resource(
            resource_type="model",
            tags=("scheduled_update",),
        )
        assert model_requires_scheduling_tag(r, default_context) is None

    def test_clean_model_with_frequent_update_tag(self, make_resource, default_context):
        r = make_resource(
            resource_type="model",
            tags=("frequent_update",),
        )
        assert model_requires_scheduling_tag(r, default_context) is None

    def test_ignores_sources(self, make_resource, default_context):
        r = make_resource(resource_type="source", tags=())
        assert model_requires_scheduling_tag(r, default_context) is None

    def test_ignores_exposures(self, make_resource, default_context):
        r = make_resource(resource_type="exposure", tags=())
        assert model_requires_scheduling_tag(r, default_context) is None
