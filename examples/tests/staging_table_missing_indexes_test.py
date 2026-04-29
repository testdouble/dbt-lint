"""Tests for staging_table_missing_indexes custom rule."""

from staging_table_missing_indexes import staging_table_missing_indexes


class TestStagingTableMissingIndexes:
    def test_flags_table_staging_without_indexes(self, make_resource, default_context):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="table",
            config={},
        )
        v = staging_table_missing_indexes(r, default_context)
        assert v is not None
        assert "indexes" in v.message

    def test_flags_incremental_staging_without_indexes(
        self, make_resource, default_context
    ):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="incremental",
            config={},
        )
        assert staging_table_missing_indexes(r, default_context) is not None

    def test_clean_table_staging_with_indexes(self, make_resource, default_context):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="table",
            config={"indexes": [{"columns": ["patient_id"], "unique": True}]},
        )
        assert staging_table_missing_indexes(r, default_context) is None

    def test_ignores_view_staging(self, make_resource, default_context):
        r = make_resource(
            resource_type="model",
            model_type="staging",
            materialization="view",
            config={},
        )
        assert staging_table_missing_indexes(r, default_context) is None

    def test_ignores_non_staging(self, make_resource, default_context):
        r = make_resource(
            resource_type="model",
            model_type="marts",
            materialization="table",
            config={},
        )
        assert staging_table_missing_indexes(r, default_context) is None
