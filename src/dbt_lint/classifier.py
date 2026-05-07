"""Classify dbt models into types (staging, intermediate, marts, base, other)."""

from __future__ import annotations

from typing import Any

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


def classify_model_type(name: str, file_path: str, params: dict[str, Any]) -> str:
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
