#!/usr/bin/env python3
"""Offline checks for Fabric Git item definitions; no Fabric credentials required."""

from __future__ import annotations

import json
import sys
from pathlib import Path


SUPPORTED_TYPES = {"DataPipeline", "Lakehouse", "Notebook", "Warehouse"}


def fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> None:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "Fabric_Ecommerce_Return_Analysis")
    if not root.is_dir():
        fail(f"Fabric repository directory does not exist: {root}")

    item_dirs = sorted(path.parent for path in root.rglob(".platform"))
    if not item_dirs:
        fail(f"No Fabric items with .platform metadata found below {root}")

    logical_ids: dict[str, Path] = {}
    for item_dir in item_dirs:
        platform_path = item_dir / ".platform"
        try:
            platform = json.loads(platform_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            fail(f"Invalid JSON in {platform_path}: {exc}")

        metadata = platform.get("metadata", {})
        config = platform.get("config", {})
        item_type = metadata.get("type")
        display_name = metadata.get("displayName")
        logical_id = config.get("logicalId")

        if item_type not in SUPPORTED_TYPES:
            fail(f"Unsupported or missing item type in {platform_path}: {item_type!r}")
        if item_dir.suffix != f".{item_type}":
            fail(f"Folder suffix does not match metadata type: {item_dir}")
        if not display_name:
            fail(f"Missing metadata.displayName in {platform_path}")
        if not logical_id:
            fail(f"Missing config.logicalId in {platform_path}")
        if logical_id in logical_ids:
            fail(f"Duplicate logicalId {logical_id} in {platform_path} and {logical_ids[logical_id]}")
        logical_ids[logical_id] = platform_path

    json_files = sorted(root.rglob("*.json")) + sorted(root.rglob(".platform"))
    for json_path in json_files:
        try:
            json.loads(json_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            fail(f"Invalid JSON in {json_path}: {exc}")

    required = {
        "DataPipeline": "pipeline-content.json",
        "Warehouse": "*.sqlproj",
        "Notebook": "notebook-content.py",
    }
    for item_dir in item_dirs:
        item_type = item_dir.suffix.removeprefix(".")
        pattern = required.get(item_type)
        if pattern and not any(item_dir.glob(pattern)):
            fail(f"{item_dir} is missing required content matching {pattern}")

    print(f"Validated {len(item_dirs)} Fabric items and {len(json_files)} JSON metadata files.")


if __name__ == "__main__":
    main()

