"""Tests for staging_no_cte_wrapping custom rule."""

from staging_no_cte_wrapping import staging_no_cte_wrapping


class TestStagingNoCteWrapping:
    def test_flags_staging_with_cte(self, make_resource, default_config):
        sql = (
            "WITH source AS (\n"
            "  SELECT * FROM {{ source('ft', 'tbl') }}\n"
            ")\nSELECT * FROM source"
        )
        r = make_resource(
            resource_type="model",
            model_type="staging",
            raw_code=sql,
        )
        v = staging_no_cte_wrapping(r, default_config)
        assert v is not None
        assert "CTE" in v.message

    def test_flags_lowercase_with(self, make_resource, default_config):
        sql = (
            "with src as (\n"
            "  select * from {{ source('ft', 'tbl') }}\n"
            ")\nselect * from src"
        )
        r = make_resource(
            resource_type="model",
            model_type="staging",
            raw_code=sql,
        )
        assert staging_no_cte_wrapping(r, default_config) is not None

    def test_clean_staging_bare_select(self, make_resource, default_config):
        sql = "SELECT\n  patient_id,\n  name\nFROM {{ source('ft', 'patients') }}"
        r = make_resource(
            resource_type="model",
            model_type="staging",
            raw_code=sql,
        )
        assert staging_no_cte_wrapping(r, default_config) is None

    def test_ignores_non_staging(self, make_resource, default_config):
        sql = (
            "WITH source AS (\n"
            "  SELECT * FROM {{ ref('stg_orders') }}\n"
            ")\nSELECT * FROM source"
        )
        r = make_resource(
            resource_type="model",
            model_type="intermediate",
            raw_code=sql,
        )
        assert staging_no_cte_wrapping(r, default_config) is None

    def test_ignores_with_inside_string(self, make_resource, default_config):
        """WITH appearing mid-query (not at start) isn't a CTE."""
        sql = "SELECT * FROM {{ source('ft', 'tbl') }} WHERE status = 'with_issues'"
        r = make_resource(
            resource_type="model",
            model_type="staging",
            raw_code=sql,
        )
        assert staging_no_cte_wrapping(r, default_config) is None
