"""Shared test helpers (non-fixture) for the dbt-lint test suite."""


def fixture_manifest_dict() -> dict:
    """Build a minimal valid manifest.json dict.

    Graph shape:
        source.pkg.raw.users
            -> model.pkg.stg_users (view, described, PK tested)
                -> model.pkg.fct_orders (table, NOT described, public, no contract)
                    -> exposure.pkg.dashboard

    Used by test_cli.py (writes to disk) and test_pipeline.py (in-memory).
    """
    return {
        "metadata": {
            "dbt_schema_version": ("https://schemas.getdbt.com/dbt/manifest/v11.json"),
        },
        "nodes": {
            "model.pkg.stg_users": {
                "unique_id": "model.pkg.stg_users",
                "name": "stg_users",
                "resource_type": "model",
                "original_file_path": "models/staging/stg_users.sql",
                "raw_code": "select * from {{ ref('raw_users') }}",
                "description": "Staged users from raw source.",
                "schema": "public",
                "database": "analytics",
                "access": "protected",
                "contract": {"enforced": False},
                "config": {
                    "materialized": "view",
                    "tags": [],
                    "meta": {},
                },
                "columns": {
                    "user_id": {
                        "name": "user_id",
                        "description": "Primary key",
                    },
                },
            },
            "model.pkg.fct_orders": {
                "unique_id": "model.pkg.fct_orders",
                "name": "fct_orders",
                "resource_type": "model",
                "original_file_path": "models/marts/fct_orders.sql",
                "raw_code": "select * from {{ ref('stg_users') }}",
                "description": "",
                "schema": "public",
                "database": "analytics",
                "access": "public",
                "contract": {"enforced": False},
                "config": {
                    "materialized": "table",
                    "tags": [],
                    "meta": {},
                },
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "description": "",
                    },
                },
            },
            "test.pkg.unique_stg_users_user_id": {
                "unique_id": "test.pkg.unique_stg_users_user_id",
                "name": "unique_stg_users_user_id",
                "resource_type": "test",
                "test_metadata": {
                    "name": "unique",
                    "namespace": "dbt",
                },
                "attached_node": "model.pkg.stg_users",
            },
            "test.pkg.not_null_stg_users_user_id": {
                "unique_id": "test.pkg.not_null_stg_users_user_id",
                "name": "not_null_stg_users_user_id",
                "resource_type": "test",
                "test_metadata": {
                    "name": "not_null",
                    "namespace": "dbt",
                },
                "attached_node": "model.pkg.stg_users",
            },
        },
        "sources": {
            "source.pkg.raw.users": {
                "unique_id": "source.pkg.raw.users",
                "name": "users",
                "resource_type": "source",
                "original_file_path": "models/staging/_sources.yml",
                "source_description": "Raw user data from app DB.",
                "description": "Users table in raw schema.",
                "schema": "raw",
                "database": "analytics",
                "meta": {},
                "freshness": {
                    "warn_after": {"count": 24, "period": "hour"},
                    "error_after": None,
                },
            },
        },
        "exposures": {
            "exposure.pkg.dashboard": {
                "unique_id": "exposure.pkg.dashboard",
                "name": "dashboard",
                "resource_type": "exposure",
                "original_file_path": "models/exposures/dashboard.yml",
            },
        },
        "parent_map": {
            "model.pkg.stg_users": ["source.pkg.raw.users"],
            "model.pkg.fct_orders": ["model.pkg.stg_users"],
            "exposure.pkg.dashboard": ["model.pkg.fct_orders"],
        },
    }
