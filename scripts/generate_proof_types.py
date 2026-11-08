#!/usr/bin/env python3
"""Generate a deliberately small TypeScript DTO subset from the OpenAPI source."""
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

def render(schema):
    if "$ref" in schema:
        return schema["$ref"].split("/")[-1]
    kind = schema["type"]
    if kind == "array":
        return "Array<" + render(schema["items"]) + ">"
    if kind == "object":
        required = schema.get("required", [])
        return "{ " + "; ".join(k + ("" if k in required else "?") + ": " + render(v)
                                for k, v in schema["properties"].items()) + " }"
    return {"string": "string", "boolean": "boolean", "integer": "number"}[kind]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    contract = json.loads((ROOT / "docs/release/v2.0/proof-openapi.json").read_text())
    text = "// Generated from proof-openapi.json; do not edit.\n"
    text += "\n".join("export type " + name + " = " + render(schema) + ";"
                      for name, schema in contract["components"]["schemas"].items()) + "\n"
    target = ROOT / "web/src/api.ts"
    if args.check:
        if not target.is_file() or target.read_text() != text:
            raise SystemExit("OpenAPI/TypeScript DTO drift; run scripts/generate_proof_types.py")
    else:
        target.write_text(text)

if __name__ == "__main__":
    main()
