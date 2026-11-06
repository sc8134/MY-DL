"""Mirror URL support: fallback, parallel race, fastest-probe strategies.

Stdlib only. Uses urllib for probing; integrates with existing HTTPClient.
"""

from __future__ import annotations

import asyncio
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Optional

from .http import ClientConfig, HTTPClient, probe


class MirrorStrategy(Enum):
    FALLBACK = "fallback"       # Sequential: try each until success
    PARALLEL = "parallel"       # Race: start all, use first success
    FASTEST = "fastest"         # Probe all, pick lowest latency


@dataclass
class MirrorProbeResult:
    url: str
    latency_ms: float | None = None
    size: int | None = None
    accept_ranges: bool = False
    error: str | None = None
    final_url: str | None = None


@dataclass
class MirrorSet:
    urls: list[str]
    strategy: MirrorStrategy = MirrorStrategy.FALLBACK
    probe_timeout: float = 5.0
    probe_concurrency: int = 8

    def __post_init__(self) -> None:
        if not self.urls:
            raise ValueError("MirrorSet requires at least one URL")
        # Deduplicate while preserving order
        seen = set()
        uniq = []
        for u in self.urls:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        self.urls = uniq

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "MirrorSet":
        urls = config.get("urls") or []
        if isinstance(urls, str):
            urls = [urls]
        strategy = MirrorStrategy(config.get("strategy", "fallback"))
        probe_timeout = float(config.get("probe_timeout", 5.0))
        probe_concurrency = int(config.get("probe_concurrency", 8))
        return cls(urls=urls, strategy=strategy, probe_timeout=probe_timeout, probe_concurrency=probe_concurrency)

    def iter_candidates(self) -> Iterator[str]:
        """Yield URLs in order for fallback strategy."""
        yield from self.urls

    def _probe_one(self, url: str, config: ClientConfig | None = None) -> MirrorProbeResult:
        t0 = time.monotonic()
        try:
            client = HTTPClient(config)
            # Use a short timeout for probing
            probe_config = ClientConfig(
                timeout=self.probe_timeout,
                retries=0,
                backoff=0,
                max_redirects=config.max_redirects if config else 10,
                headers=config.headers if config else {},
                insecure=config.insecure if config else False,
                ca_file=config.ca_file if config else None,
            )
            info = probe(url, probe_config)
            latency = (time.monotonic() - t0) * 1000
            return MirrorProbeResult(
                url=url,
                latency_ms=latency,
                size=info.get("size"),
                accept_ranges=info.get("accept_ranges", False),
                final_url=info.get("url"),
            )
        except Exception as e:
            return MirrorProbeResult(url=url, error=str(e))

    def probe_all(self, config: ClientConfig | None = None) -> list[MirrorProbeResult]:
        """Probe all mirrors concurrently, return results sorted by latency (successful first)."""
        results: list[MirrorProbeResult] = []
        with ThreadPoolExecutor(max_workers=min(self.probe_concurrency, len(self.urls))) as ex:
            futures = {ex.submit(self._probe_one, u, config): u for u in self.urls}
            for fut in as_completed(futures):
                results.append(fut.result())
        # Sort: successful (by latency asc), then failed
        results.sort(key=lambda r: (r.error is not None, r.latency_ms or float("inf")))
        return results

    def select_best(self, config: ClientConfig | None = None) -> str:
        """FASTEST strategy: probe all, return URL with lowest latency."""
        results = self.probe_all(config)
        for r in results:
            if r.error is None:
                return r.final_url or r.url
        # All failed; return first as fallback
        return self.urls[0]

    def race_download(
        self,
        download_fn: Callable[[str], Any],  # function(url) -> result
        *,
        max_workers: int | None = None,
    ) -> tuple[str, Any]:  # (chosen_url, result)
        """PARALLEL strategy: start download on all mirrors, return first success."""
        max_workers = max_workers or min(4, len(self.urls))
        errors: list[Exception] = []

        # Since downloader is sync, use ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            # Submit download tasks directly
            futures = {ex.submit(download_fn, u): u for u in self.urls}
            for fut in as_completed(futures):
                url = futures[fut]
                try:
                    result = fut.result()
                    # Cancel remaining
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    return url, result
                except Exception as e:
                    errors.append(e)
                    continue
        raise RuntimeError(f"All mirrors failed: {errors}")


def parse_mirror_file(path: str) -> list[str]:
    """Parse a file with one URL per line; # comments allowed."""
    urls: list[str] = []
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            urls.append(s.split()[0])
    return urls


def build_mirror_set(
    primary_url: str,
    mirror_args: list[str] | None,
    mirror_file: str | None,
    strategy: str | MirrorStrategy = "fallback",
    probe_timeout: float = 5.0,
) -> MirrorSet:
    urls = [primary_url]
    if mirror_args:
        urls.extend(mirror_args)
    if mirror_file:
        urls.extend(parse_mirror_file(mirror_file))
    if isinstance(strategy, str):
        strategy = MirrorStrategy(strategy)
    return MirrorSet(urls=urls, strategy=strategy, probe_timeout=probe_timeout)