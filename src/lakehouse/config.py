"""Typed access to the metadata-driven pipeline configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

VALID_LOAD_TYPES = {"scd1", "scd2", "append_dedup"}


@dataclass(frozen=True)
class EntityConfig:
    name: str
    format: str
    business_keys: list[str]
    load_type: str
    schema: dict[str, str]
    sequence_column: str
    read_options: dict[str, str] = field(default_factory=dict)
    quality_rules: list[dict[str, Any]] = field(default_factory=list)
    partition_by: list[str] = field(default_factory=list)
    derived_columns: dict[str, str] = field(default_factory=dict)
    cdc_op_column: str | None = None
    scd2_tracked_columns: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.load_type not in VALID_LOAD_TYPES:
            raise ValueError(f"{self.name}: unknown load_type '{self.load_type}'")
        if self.load_type == "scd2" and not self.scd2_tracked_columns:
            raise ValueError(f"{self.name}: scd2 requires scd2_tracked_columns")
        missing = [k for k in self.business_keys if k not in self.schema]
        if missing:
            raise ValueError(f"{self.name}: business keys {missing} not in schema")


@dataclass(frozen=True)
class PipelineConfig:
    root: str
    storage: dict[str, str]
    entities: list[EntityConfig]
    fx_rates_to_usd: dict[str, float]
    gold: dict[str, Any]

    def path(self, layer: str, *parts: str) -> str:
        """Build a path for a layer; works for local paths and abfss:// URIs."""
        return "/".join([self.root.rstrip("/"), self.storage[layer], *parts])

    def entity(self, name: str) -> EntityConfig:
        for e in self.entities:
            if e.name == name:
                return e
        raise KeyError(name)


def load_config(path: str | Path, root_override: str | None = None) -> PipelineConfig:
    raw = yaml.safe_load(Path(path).read_text())
    storage = dict(raw["storage"])
    root = root_override or os.environ.get("LAKEHOUSE_ROOT") or storage.pop("root")
    storage.pop("root", None)
    if "://" not in root:
        root = str(Path(root).resolve())
    entities = [EntityConfig(**e) for e in raw["entities"]]
    return PipelineConfig(
        root=root,
        storage=storage,
        entities=entities,
        fx_rates_to_usd={k: float(v) for k, v in raw.get("fx_rates_to_usd", {}).items()},
        gold=raw.get("gold", {}),
    )
