"""Tests for avoid_select_distinct custom rule."""

from avoid_select_distinct import avoid_select_distinct


class TestAvoidSelectDistinct:
    def test_flags_select_distinct(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            raw_code="SELECT DISTINCT customer_id FROM orders",
        )
        v = avoid_select_distinct(r, default_config)
        assert v is not None
        assert "SELECT DISTINCT" in v.message

    def test_flags_case_insensitive(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            raw_code="select distinct customer_id from orders",
        )
        assert avoid_select_distinct(r, default_config) is not None

    def test_flags_with_newline(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            raw_code="SELECT\n  DISTINCT customer_id FROM orders",
        )
        assert avoid_select_distinct(r, default_config) is not None

    def test_clean_no_distinct(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            raw_code="SELECT customer_id FROM orders GROUP BY 1",
        )
        assert avoid_select_distinct(r, default_config) is None

    def test_ignores_count_distinct(self, make_resource, default_config):
        r = make_resource(
            resource_type="model",
            raw_code="SELECT COUNT(DISTINCT customer_id) FROM orders",
        )
        assert avoid_select_distinct(r, default_config) is None

    def test_ignores_sources(self, make_resource, default_config):
        r = make_resource(
            resource_type="source",
            raw_code="SELECT DISTINCT x FROM y",
        )
        assert avoid_select_distinct(r, default_config) is None
