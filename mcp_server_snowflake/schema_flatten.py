# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use it except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Flatten JSON Schema so it is compatible with Claude (and other providers) that
reject oneOf/allOf/anyOf at the top level of tool input_schema.

When clients convert schemas to Zod, allOf becomes ZodIntersection, which
triggers "does not support zod type: ZodIntersection" with claude-sonnet-4-5.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


def _ensure_description_string(schema: dict[str, Any]) -> None:
    """Ensure description is never None (JSON Schema requires string if present). Mutates in place."""
    if "description" in schema and schema["description"] is None:
        schema["description"] = ""


def _is_object_schema(schema: dict[str, Any]) -> bool:
    """True if schema is or resolves to an object type."""
    if schema.get("type") == "object":
        return True
    if "allOf" in schema:
        return all(_is_object_schema(s) for s in schema["allOf"])
    if "oneOf" in schema or "anyOf" in schema:
        for sub in schema.get("oneOf", schema.get("anyOf", [])):
            if not _is_object_schema(sub):
                return False
        return True
    return False


def _merge_object_schemas(schemas: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge multiple object JSON schemas into one (combined properties and required)."""
    merged_props: dict[str, Any] = {}
    merged_required: list[str] = []

    for s in schemas:
        s = deepcopy(s)
        # Resolve $ref in-place would require defs; for our use we expect inline or already resolved
        if s.get("type") != "object" and "allOf" in s:
            s = _flatten_schema_node(s)
        if s.get("type") != "object":
            continue
        props = s.get("properties", {})
        for k, v in props.items():
            if k not in merged_props:
                merged_props[k] = _flatten_schema_node(v)
            else:
                # Already present: optionally merge (take first for simplicity)
                pass
        for r in s.get("required", []):
            if r not in merged_required:
                merged_required.append(r)

    return {
        "type": "object",
        "properties": merged_props,
        **({"required": merged_required} if merged_required else {}),
    }


def _flatten_schema_node(schema: dict[str, Any], depth: int = 0) -> dict[str, Any]:
    """Recursively flatten oneOf/anyOf/allOf in a schema node. In-place style; returns new dict."""
    if depth > 30:
        return schema
    schema = deepcopy(schema)

    if "allOf" in schema:
        subs = [_flatten_schema_node(s, depth + 1) for s in schema["allOf"]]
        if all(_is_object_schema(s) for s in subs):
            return _merge_object_schemas(subs)
        # Non-object allOf: merge if we can, else generic object
        if subs and all(s.get("type") == "object" for s in subs):
            return _merge_object_schemas(subs)
        return {"type": "object", "description": schema.get("description", "JSON object")}

    if "oneOf" in schema or "anyOf" in schema:
        key = "oneOf" if "oneOf" in schema else "anyOf"
        subs = [_flatten_schema_node(s, depth + 1) for s in schema[key]]
        if all(_is_object_schema(s) for s in subs):
            return _merge_object_schemas(subs)
        # anyOf/oneOf with primitives (e.g. string | null): keep first primitive type so
        # optional string params stay "type": "string" instead of becoming "type": "object"
        primitives = ("string", "number", "integer", "boolean", "array")
        for s in subs:
            t = s.get("type")
            if t is None:
                continue
            if isinstance(t, list):
                for u in t:
                    if u in primitives:
                        return {
                            "type": u,
                            "description": schema.get("description") or s.get("description") or "",
                        }
                continue
            if t in primitives:
                return {
                    "type": t,
                    "description": schema.get("description") or s.get("description") or "",
                }
            if t == "null":
                continue
        # Mixed or unknown: fallback to permissive object
        return {
            "type": "object",
            "description": schema.get("description") or "JSON object (structure may vary)",
        }

    # Recursively flatten nested properties
    if "properties" in schema:
        schema["properties"] = {
            k: _flatten_schema_node(v, depth + 1)
            for k, v in schema["properties"].items()
        }
        for v in schema["properties"].values():
            _ensure_description_string(v)
    if "items" in schema and isinstance(schema["items"], dict):
        schema["items"] = _flatten_schema_node(schema["items"], depth + 1)
        _ensure_description_string(schema["items"])
    if "additionalProperties" in schema and isinstance(schema["additionalProperties"], dict):
        schema["additionalProperties"] = _flatten_schema_node(
            schema["additionalProperties"], depth + 1
        )
        _ensure_description_string(schema["additionalProperties"])

    _ensure_description_string(schema)
    return schema


def flatten_tool_input_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """
    Flatten a tool input JSON Schema so it has no oneOf/allOf/anyOf at the top level.

    Claude (and some clients) reject tool input_schema that use these keywords at the
    root. This merges or simplifies the schema so it is a single object schema
    with type/properties/required.

    Parameters
    ----------
    schema : dict
        The tool's inputSchema (parameters) as returned by Pydantic/FastMCP.

    Returns
    -------
    dict
        A new schema suitable for tool input_schema (no top-level composition).
    """
    if not schema:
        return {"type": "object", "properties": {}}
    root = _flatten_schema_node(schema)
    # Ensure root is always an object (MCP tool input is one object)
    if root.get("type") != "object":
        return {"type": "object", "properties": {}, "description": root.get("description")}
    return root
