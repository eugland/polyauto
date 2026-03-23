from __future__ import annotations

import json
from typing import Any


def _merge_schemas(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    if left == right:
        return left

    left_type = left.get("type")
    right_type = right.get("type")
    if left_type != right_type:
        left_options = left["oneOf"] if "oneOf" in left else [left]
        right_options = right["oneOf"] if "oneOf" in right else [right]
        merged: list[dict[str, Any]] = []
        for option in left_options + right_options:
            if option not in merged:
                merged.append(option)
        return {"oneOf": merged}

    if left_type == "object":
        left_props = left.get("properties", {})
        right_props = right.get("properties", {})
        keys = set(left_props) | set(right_props)
        merged_props: dict[str, Any] = {}
        for key in keys:
            if key in left_props and key in right_props:
                merged_props[key] = _merge_schemas(left_props[key], right_props[key])
            elif key in left_props:
                merged_props[key] = left_props[key]
            else:
                merged_props[key] = right_props[key]
        return {"type": "object", "properties": merged_props}

    if left_type == "array":
        left_items = left.get("items", {"type": "null"})
        right_items = right.get("items", {"type": "null"})
        return {"type": "array", "items": _merge_schemas(left_items, right_items)}

    return left


def infer_json_schema(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "boolean"}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"type": "integer"}
    if isinstance(value, float):
        return {"type": "number"}
    if isinstance(value, str):
        return {"type": "string"}
    if isinstance(value, list):
        if not value:
            return {"type": "array", "items": {}}
        item_schema = infer_json_schema(value[0])
        for item in value[1:]:
            item_schema = _merge_schemas(item_schema, infer_json_schema(item))
        return {"type": "array", "items": item_schema}
    if isinstance(value, dict):
        properties = {k: infer_json_schema(v) for k, v in value.items()}
        return {
            "type": "object",
            "properties": properties,
            "required": list(value.keys()),
            "additionalProperties": True,
        }
    return {}


def write_schema(payload: dict[str, Any], path: str) -> None:
    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "PolymarketTemperatureMarketsResponse",
        **infer_json_schema(payload),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
        f.write("\n")

