"""Parse manifest.json into Resources and DirectEdges."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from dbt_linter.config import Config
from dbt_linter.models import DirectEdge, Resource


def _check_schema_version(manifest: dict) -> None:
    """Validate manifest schema version is v11+. Exits if older or missing."""
    try:
        version_url = manifest["metadata"]["dbt_schema_version"]
    except (KeyError, TypeError):
        sys.exit("Error: manifest.json missing metadata.dbt_schema_version")

    match = re.search(r"/v(\d+)", version_url)
    if not match or int(match.group(1)) < 11:
        sys.exit(
            f"Error: manifest schema {version_url} is below v11. "
            "dbt-linter requires dbt 1.6+ (manifest v11+)."
        )


def _build_test_index(manifest: dict) -> dict[str, list[dict]]:
    """Map attached_node -> list of test_metadata dicts."""
    index: dict[str, list[dict]] = {}
    for node in manifest["nodes"].values():
        if node.get("resource_type") == "test" and node.get("test_metadata"):
            attached = node.get("attached_node")
            if attached:
                index.setdefault(attached, []).append(node["test_metadata"])
    return index


# Prefix -> model_type mapping: (config_key_for_prefixes, model_type_name)
_PREFIX_KEYS = [
    ("staging_prefixes", "staging"),
    ("intermediate_prefixes", "intermediate"),
    ("marts_prefixes", "marts"),
    ("base_prefixes", "base"),
    ("other_prefixes", "other"),
]

# Folder -> model_type mapping: (config_key_for_folder, model_type_name)
_FOLDER_KEYS = [
    ("staging_folder_name", "staging"),
    ("intermediate_folder_name", "intermediate"),
    ("marts_folder_name", "marts"),
    ("base_folder_name", "base"),
]


def _classify_model_type(name: str, file_path: str, params: dict[str, Any]) -> str:
    """Classify a model's type using a two-pass heuristic: prefix then directory."""
    # Pass 1: prefix match
    for key, model_type in _PREFIX_KEYS:
        for prefix in params[key]:
            if name.startswith(prefix):
                return model_type

    # Pass 2: directory match
    parts = file_path.split("/")
    for key, model_type in _FOLDER_KEYS:
        folder_name = params[key]
        if folder_name in parts:
            return model_type

    return "other"


# Identifier: unquoted word, double-quoted, or backtick-quoted
_IDENT = r'(?:\w+|"[^"]+"|`[^`]+`)'

# Patterns that indicate hard-coded table references after FROM/JOIN keywords.
# Matches: schema.table, database.schema.table, or {{ var(...) }}.anything
_HARD_CODED_RE = re.compile(
    r"(?i)(?:FROM|JOIN)\s+(?:"
    r"\{\{\s*var\s*\("  # {{ var( ... pattern
    r"|"
    + _IDENT
    + r"\."
    + _IDENT
    + r"(?:\."
    + _IDENT
    + r")?"  # schema.table or db.schema.table
    r")",
)


def _has_hard_coded_references(raw_code: str) -> bool:
    """Detect hard-coded table/schema references in SQL via regex."""
    if not raw_code:
        return False
    # Strip Jinja comments before scanning
    stripped = re.sub(r"\{#.*?#\}", "", raw_code, flags=re.DOTALL)
    return _HARD_CODED_RE.search(stripped) is not None


def _is_primary_key_tested(
    tests: list[dict], primary_key_test_macros: list[list[str]]
) -> bool:
    """Check if a model's tests satisfy any primary_key_test_macros combination.

    Each inner list is a set of macro names
    (e.g. ["dbt.test_unique", "dbt.test_not_null"])
    that must ALL be present in the model's test_metadata to qualify.
    Macro name format: "namespace.test_name".
    """
    if not tests:
        return False

    # Build set of fully-qualified test names from test_metadata entries
    present = {f"{t['namespace']}.test_{t['name']}" for t in tests}

    return any(
        all(macro in present for macro in combo) for combo in primary_key_test_macros
    )


def _has_relationship_tests(tests: list[dict]) -> bool:
    """Check if any relationship/referential integrity tests are present."""
    return any(t.get("name") == "relationships" for t in tests)


def _extract_skip_rules(meta: dict) -> frozenset[str]:
    """Extract skip rules from meta.dbt-linter.skip list."""
    linter_meta = meta.get("dbt-linter", {})
    skip_list = linter_meta.get("skip", [])
    return frozenset(skip_list) if skip_list else frozenset()


def _model_to_resource(
    node: dict, test_index: dict[str, list[dict]], params: dict[str, Any]
) -> Resource:
    """Convert a model node dict to a Resource."""
    unique_id = node["unique_id"]
    name = node["name"]
    file_path = node["original_file_path"]
    config = node.get("config", {})
    meta = config.get("meta", {})
    columns = node.get("columns", {})
    tests = test_index.get(unique_id, [])

    return Resource(
        resource_id=unique_id,
        resource_name=name,
        resource_type="model",
        file_path=file_path,
        model_type=_classify_model_type(name, file_path, params),
        materialization=config.get("materialized", ""),
        schema_name=node.get("schema", ""),
        database=node.get("database", ""),
        is_described=bool(node.get("description", "")),
        is_public=node.get("access") == "public",
        is_contract_enforced=bool(node.get("contract", {}).get("enforced")),
        hard_coded_references=_has_hard_coded_references(node.get("raw_code", "")),
        number_of_columns=len(columns),
        number_of_documented_columns=sum(
            1 for c in columns.values() if c.get("description", "")
        ),
        is_freshness_enabled=False,
        is_primary_key_tested=_is_primary_key_tested(
            tests, params["primary_key_test_macros"]
        ),
        has_relationship_tests=_has_relationship_tests(tests),
        tags=tuple(config.get("tags", [])),
        meta=meta,
        skip_rules=_extract_skip_rules(meta),
    )


def _source_to_resource(source: dict) -> Resource:
    """Convert a source node dict to a Resource."""
    meta = source.get("meta", {})
    freshness = source.get("freshness")
    is_fresh = False
    if freshness:
        is_fresh = bool(freshness.get("warn_after") or freshness.get("error_after"))

    source_desc_populated = bool(source.get("source_description", ""))
    enriched_meta = {**meta, "source_description_populated": source_desc_populated}

    return Resource(
        resource_id=source["unique_id"],
        resource_name=source["name"],
        resource_type="source",
        file_path=source.get("original_file_path", ""),
        model_type="",
        materialization="",
        schema_name=source.get("schema", ""),
        database=source.get("database", ""),
        is_described=bool(source.get("description", "")),
        is_public=False,
        is_contract_enforced=False,
        hard_coded_references=False,
        number_of_columns=0,
        number_of_documented_columns=0,
        is_freshness_enabled=is_fresh,
        is_primary_key_tested=False,
        has_relationship_tests=False,
        tags=(),
        meta=enriched_meta,
        skip_rules=_extract_skip_rules(meta),
    )


def _exposure_to_resource(exposure: dict) -> Resource:
    """Convert an exposure node dict to a Resource."""
    return Resource(
        resource_id=exposure["unique_id"],
        resource_name=exposure["name"],
        resource_type="exposure",
        file_path=exposure.get("original_file_path", ""),
        model_type="",
        materialization="",
        schema_name="",
        database="",
        is_described=False,
        is_public=False,
        is_contract_enforced=False,
        hard_coded_references=False,
        number_of_columns=0,
        number_of_documented_columns=0,
        is_freshness_enabled=False,
        is_primary_key_tested=False,
        has_relationship_tests=False,
        tags=(),
        meta={},
        skip_rules=frozenset(),
    )


def _extract_edges(parent_map: dict[str, list[str]]) -> list[DirectEdge]:
    """Convert parent_map to a list of DirectEdge records."""
    edges = []
    for child, parents in parent_map.items():
        for parent in parents:
            edges.append(DirectEdge(parent=parent, child=child))
    return edges


def parse_manifest(
    path: Path, config: Config
) -> tuple[list[Resource], list[DirectEdge]]:
    """Parse manifest.json into Resources and DirectEdges.

    Validates schema version, extracts models/sources/exposures,
    builds test index for PK test derivation, and extracts parent_map edges.
    """
    manifest = json.loads(path.read_bytes())
    _check_schema_version(manifest)

    test_index = _build_test_index(manifest)
    params = config.params
    resources: list[Resource] = []

    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") == "model":
            resources.append(_model_to_resource(node, test_index, params))

    for source in manifest.get("sources", {}).values():
        resources.append(_source_to_resource(source))

    for exposure in manifest.get("exposures", {}).values():
        resources.append(_exposure_to_resource(exposure))

    edges = _extract_edges(manifest.get("parent_map", {}))
    return resources, edges
