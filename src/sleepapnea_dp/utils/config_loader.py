"""
Loads environment-specific configuration (config/<env>/pipeline_config.yaml merged with
config/common.yaml). No secrets are stored here — secret *names* are referenced and resolved
via Databricks secret scopes backed by Azure Key Vault at runtime.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml


@dataclass
class PipelineConfig:
    env: str
    raw: Dict[str, Any]

    def __getitem__(self, key: str) -> Any:
        return self.raw[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.raw.get(key, default)

    @property
    def catalog(self) -> str:
        return self.raw["unity_catalog"]["catalog_name"]

    @property
    def storage_account(self) -> str:
        return self.raw["storage"]["account_name"]

    def container_path(self, container: str) -> str:
        """abfss:// path for a given container, e.g. 'raw', 'bronze', 'silver', 'gold'."""
        path_template = self.raw["storage"]["containers"][container]
        return path_template.format(
            container=container,
            account_name=self.storage_account,
        )

    def secret(self, dbutils, key: str) -> str:
        """Resolve a secret by logical name via the configured Databricks secret scope."""
        scope = self.raw["security"]["secret_scope"]
        secret_key = self.raw["security"]["secrets"][key]
        return dbutils.secrets.get(scope=scope, key=secret_key)


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def load_config(env: str | None = None, config_root: str | None = None) -> PipelineConfig:
    """
    Loads config/common.yaml then deep-merges config/<env>/pipeline_config.yaml on top.

    env resolution order: explicit arg > ENV var `SLEEPAPNEA_ENV` > Databricks job widget
    (caller's responsibility to pass it in) > defaults to 'dev'.
    """
    env = env or os.environ.get("SLEEPAPNEA_ENV", "dev")
    root = Path(config_root) if config_root else Path(__file__).resolve().parents[3] / "config"

    common_path = root / "common.yaml"
    env_path = root / env / "pipeline_config.yaml"

    common_cfg = yaml.safe_load(common_path.read_text()) if common_path.exists() else {}
    env_cfg = yaml.safe_load(env_path.read_text()) if env_path.exists() else {}

    if not env_cfg:
        raise FileNotFoundError(f"No config found for env='{env}' at {env_path}")

    merged = _deep_merge(common_cfg, env_cfg)
    return PipelineConfig(env=env, raw=merged)
