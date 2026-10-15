#!/usr/bin/env python3
"""Publish approved main-branch Fabric definitions to the production workspace."""

from __future__ import annotations

import os
from pathlib import Path

from azure.identity import AzureCliCredential
from fabric_cicd import FabricWorkspace, append_feature_flag, publish_all_items


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment setting {name} is empty")
    return value


def main() -> None:
    repository_root = Path(__file__).resolve().parents[2]
    fabric_root = repository_root / "Fabric_Ecommerce_Return_Analysis"

    # Allows parameter.yml to read production values supplied by the protected
    # GitHub environment. AzureCliCredential reuses the preceding OIDC login.
    append_feature_flag("enable_environment_variable_replacement")
    target = FabricWorkspace(
        workspace_id=required_env("PROD_WORKSPACE_ID"),
        environment="PROD",
        repository_directory=str(fabric_root),
        item_type_in_scope=["Lakehouse", "Warehouse", "Notebook", "DataPipeline"],
        parameter_file_path=str(fabric_root / "parameter.yml"),
        token_credential=AzureCliCredential(),
    )

    # Intentionally do not call unpublish_all_orphan_items: deletion from the
    # production workspace requires a separate, reviewed operational decision.
    publish_all_items(target)


if __name__ == "__main__":
    main()

