"""Config loading. Stdlib only (json, tomllib on 3.11+)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import tomllib
except Exception:  # pragma: no cover
    tomllib = None  # type: ignore


def load_config(explicit: str | None = None) -> dict[str, Any]:
    paths: list[Path] = []
    if explicit:
        paths.append(Path(explicit).expanduser())
    else:
        cwd = Path.cwd()
        home = Path.home()
        for base in (cwd, home):
            for name in (".mydl.toml", "mydl.toml", ".mydl.json", "mydl.json"):
                paths.append(base / name)
    for p in paths:
        if not p.exists() or not p.is_file():
            continue
        try:
            if p.suffix.lower() == ".toml":
                if tomllib is None:
                    continue
                data = tomllib.loads(p.read_text(encoding="utf-8"))
            else:
                data = json.loads(p.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            if explicit:
                raise
            continue
    return {}


def merged_get_options(config: dict[str, Any]) -> dict[str, Any]:
    # Accept top-level or [get]/[download] sections.
    out: dict[str, Any] = {}
    for key in ("get", "download", "mydl", "defaults"):
        sec = config.get(key)
        if isinstance(sec, dict):
            out.update(sec)
    # Also merge mirrors section if present
    if "mirrors" in config and isinstance(config["mirrors"], dict):
        out["mirrors"] = config["mirrors"]
    if "ftp" in config and isinstance(config["ftp"], dict):
        out["ftp"] = config["ftp"]
    if "web" in config and isinstance(config["web"], dict):
        out["web"] = config["web"]
    for k, v in config.items():
        if not isinstance(v, dict):
            out.setdefault(k, v)
    return out
