"""Naming helpers for output paths."""

from __future__ import annotations

import os
import re
import urllib.parse
from pathlib import Path
from typing import Any

_UNSAFE = re.compile(r'[<>:"/\\|?*\x00-\x1f]+')


def sanitize_filename(name: str) -> str:
    name = _UNSAFE.sub("_", name).strip(" .")
    return name or "download"


def expand_output(
    template: str | None,
    *,
    url: str,
    info: dict[str, Any],
    index: int = 1,
    directory: str | None = None,
) -> Path | None:
    if not template:
        base = directory or "."
        return Path(base) if directory else None
    parsed = urllib.parse.urlparse(info.get("url") or url)
    filename = sanitize_filename(str(info.get("filename") or Path(parsed.path).name or "download"))
    stem = Path(filename).stem
    ext = Path(filename).suffix.lstrip(".")
    host = parsed.hostname or "host"
    mapping = {
        "filename": filename,
        "name": stem,
        "ext": ext,
        "index": str(index),
        "host": host,
    }
    out = template
    for k, v in mapping.items():
        out = out.replace("{" + k + "}", v)
    p = Path(os.path.expandvars(os.path.expanduser(out)))
    if directory and not p.is_absolute():
        p = Path(directory) / p
    # If template resolved to an existing directory or ends with separator intent, append filename.
    if (p.exists() and p.is_dir()) or str(template).endswith(("/", "\\")):
        p = p / filename
    return p
