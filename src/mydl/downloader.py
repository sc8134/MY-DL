"""Download core.

Windows note: os.pwrite is not available. Parallel segment writes must use either
one open file handle per worker/segment (each handle seeks independently) or a lock
around seek+write on a shared handle. This module uses one handle per segment worker
opened in r+b mode on a pre-sized .part file.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from .http import ClientConfig, HTTPClient, probe
from .mirrors import MirrorSet, MirrorStrategy, build_mirror_set
from .naming import expand_output
from .ratelimit import SpeedLimiter


@dataclass
class Progress:
    downloaded: int = 0
    total: int | None = None
    speed_bps: float = 0.0
    eta_seconds: float | None = None


@dataclass
class _State:
    url: str
    final_url: str | None = None
    size: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    accept_ranges: bool = False
    mode: str = "single"
    segments: list[dict[str, int]] = field(default_factory=list)
    updated_at: float = 0.0
    sha256: str | None = None


class IntegrityError(ValueError):
    pass



ProgressHook = Callable[[Progress], None]


def _fmt_bytes(n: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    if i == 0:
        return f"{int(n)} {units[i]}"
    return f"{n:.1f} {units[i]}"


def _fmt_eta(seconds: float | None) -> str:
    if seconds is None or seconds != seconds or seconds == float("inf"):
        return "--:--"
    s = max(0, int(seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m:02d}:{sec:02d}"


def default_progress_hook(p: Progress) -> None:
    total = _fmt_bytes(p.total) if p.total else "?"
    pct = f"{(p.downloaded / p.total * 100):5.1f}%" if p.total else "  ?.%"
    line = (
        f"\r{pct} {_fmt_bytes(p.downloaded)}/{total} "
        f"{_fmt_bytes(p.speed_bps)}/s ETA {_fmt_eta(p.eta_seconds)}"
    )
    sys.stderr.write(line)
    sys.stderr.flush()
    if p.total and p.downloaded >= p.total:
        sys.stderr.write("\n")


class _ProgressTracker:
    def __init__(self, total: int | None, hook: ProgressHook | None) -> None:
        self.total = total
        self.hook = hook
        self.lock = threading.Lock()
        self.downloaded = 0
        self._t0 = time.monotonic()
        self._win_t = self._t0
        self._win_bytes = 0
        self.speed = 0.0

    def add(self, n: int) -> None:
        if n <= 0:
            return
        with self.lock:
            self.downloaded += n
            self._win_bytes += n
            now = time.monotonic()
            dt = now - self._win_t
            if dt >= 0.25:
                self.speed = self._win_bytes / dt
                self._win_bytes = 0
                self._win_t = now
            self._emit_locked()

    def set_downloaded(self, n: int) -> None:
        with self.lock:
            self.downloaded = n
            self._emit_locked()

    def emit(self) -> None:
        with self.lock:
            self._emit_locked()

    def _emit_locked(self) -> None:
        if not self.hook:
            return
        eta = ((self.total - self.downloaded) / self.speed) if self.total and self.speed > 0 else None
        self.hook(Progress(downloaded=self.downloaded, total=self.total, speed_bps=self.speed, eta_seconds=eta))


def _validators_match(state: _State, old: _State | None) -> bool:
    if old is None:
        return True
    if state.etag and old.etag:
        return state.etag == old.etag
    if state.last_modified and old.last_modified:
        return state.last_modified == old.last_modified
    if state.size and old.size:
        return state.size == old.size
    return True


def _load_state(path: Path) -> _State | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        out = _State(url=str(data.get("url") or ""))
        for k in _State.__dataclass_fields__:
            if k in data and data[k] is not None:
                setattr(out, k, data[k])
        if not isinstance(out.segments, list):
            out.segments = []
        norm = []
        for s in out.segments:
            if not isinstance(s, dict):
                continue
            try:
                start = int(s["start"]); end = int(s["end"]); done = int(s.get("done", start))
            except Exception:
                continue
            done = max(start, min(done, end + 1))
            if start <= end:
                norm.append({"start": start, "end": end, "done": done})
        out.segments = norm
        return out
    except Exception:
        return None


def _save_state(path: Path, state: _State) -> None:
    state.updated_at = time.time()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(asdict(state), indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _parse_total(content_range: str | None) -> int | None:
    if not content_range or "/" not in content_range:
        return None
    tail = content_range.rsplit("/", 1)[1].strip()
    return int(tail) if tail.isdigit() else None


def _split_segments(total: int, connections: int, min_size: int = 1024 * 1024) -> list[dict[str, int]]:
    n = max(1, connections)
    if total // n < min_size:
        n = max(1, total // min_size)
    n = max(1, min(n, connections, total if total > 0 else 1))
    size = total // n
    segs: list[dict[str, int]] = []
    start = 0
    for i in range(n):
        end = (start + size - 1) if i < n - 1 else (total - 1)
        segs.append({"start": start, "end": end, "done": start})
        start = end + 1
    return segs


def _verify_size(path: Path, expected: int | None) -> None:
    if expected is None:
        return
    actual = path.stat().st_size
    if actual != expected:
        raise IntegrityError(f"size mismatch for {path}: expected {expected}, got {actual}")


def file_hash(path: Path, algorithm: str = "sha256") -> str:
    h = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _verify_hash(path: Path, algorithm: str, expected: str | None) -> None:
    if not expected:
        return
    actual = file_hash(path, algorithm)
    want = expected.lower().strip()
    if actual.lower() != want:
        raise IntegrityError(f"{algorithm} mismatch for {path}: expected {want}, got {actual}")


def verify_integrity(
    path: str | Path,
    *,
    size: int | None = None,
    sha256: str | None = None,
    sha1: str | None = None,
    md5: str | None = None,
) -> dict[str, Any]:
    p = Path(path)
    _verify_size(p, size)
    _verify_hash(p, "sha256", sha256)
    _verify_hash(p, "sha1", sha1)
    _verify_hash(p, "md5", md5)
    out: dict[str, Any] = {"path": str(p), "size": p.stat().st_size, "ok": True}
    if sha256:
        out["sha256"] = sha256.lower().strip()
    if sha1:
        out["sha1"] = sha1.lower().strip()
    if md5:
        out["md5"] = md5.lower().strip()
    return out


class SingleStreamDownloader:
    """Phase 2: single stream with resume, progress, ETA."""

    def __init__(
        self,
        config: ClientConfig | None = None,
        progress: ProgressHook | None = default_progress_hook,
        chunk_size: int = 256 * 1024,
        speed_limit_bps: float | None = None,
    ) -> None:
        self.config = config or ClientConfig()
        self.client = HTTPClient(self.config)
        self.progress = progress
        self.chunk_size = chunk_size
        self.limiter = SpeedLimiter(speed_limit_bps)

    def download(
        self,
        url: str,
        output: str | None = None,
        resume: bool = True,
        *,
        sha256: str | None = None,
        sha1: str | None = None,
        md5: str | None = None,
        output_dir: str | None = None,
        output_template: str | None = None,
        index: int = 1,
    ) -> str:
        info = probe(url, self.config)
        named = expand_output(output_template, url=url, info=info, index=index, directory=output_dir)
        dest = Path(output or (str(named) if named else None) or info.get("filename") or "download").expanduser()
        if dest.exists() and dest.is_dir():
            dest = dest / (info.get("filename") or "download")

        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        state_path = dest.with_name(dest.name + ".mydl.json")

        total = info.get("size")
        state = _State(
            url=url,
            final_url=info.get("url"),
            size=total,
            etag=info.get("etag"),
            last_modified=info.get("last_modified"),
            accept_ranges=bool(info.get("accept_ranges")),
            mode="single",
            sha256=(sha256.lower().strip() if sha256 else None),
        )
        existing_state = _load_state(state_path)
        if existing_state and existing_state.url not in {url, state.final_url}:
            existing_state = None

        start = 0
        mode = "wb"
        if resume and part.exists() and (not existing_state or existing_state.mode == "single"):
            start = part.stat().st_size
            if start and state.accept_ranges and _validators_match(state, existing_state):
                mode = "ab"
            elif total is not None and start >= total > 0:
                verify_integrity(part, size=total, sha256=sha256, sha1=sha1, md5=md5)
                part.replace(dest)
                state_path.unlink(missing_ok=True)
                if self.progress:
                    self.progress(Progress(downloaded=start, total=total, speed_bps=0.0))
                return str(dest)
            else:
                start = 0
                mode = "wb"
                part.unlink(missing_ok=True)
        elif part.exists() and not resume:
            part.unlink(missing_ok=True)

        _save_state(state_path, state)
        headers: dict[str, str] = {}
        if start and state.accept_ranges:
            headers["Range"] = f"bytes={start}-"
            if state.etag:
                headers["If-Range"] = state.etag
            elif state.last_modified:
                headers["If-Range"] = state.last_modified

        tracker = _ProgressTracker(total, self.progress)
        tracker.set_downloaded(start)
        downloaded = start

        try:
            with self.client.open(info["url"] or url, method="GET", headers=headers) as resp:
                status = getattr(resp, "status", 200)
                if start and status == 200:
                    downloaded = 0
                    tracker.set_downloaded(0)
                    mode = "wb"
                elif status not in {200, 206}:
                    raise urllib.error.HTTPError(info["url"] or url, status, "unexpected status", resp.headers, None)

                if status == 206:
                    parsed_total = _parse_total(resp.headers.get("Content-Range"))
                    if parsed_total is not None:
                        total = parsed_total
                        tracker.total = total
                elif total is None:
                    cl = resp.headers.get("Content-Length")
                    if cl and cl.isdigit():
                        total = int(cl)
                        tracker.total = total

                part.parent.mkdir(parents=True, exist_ok=True)
                with open(part, mode) as f:
                    while True:
                        chunk = resp.read(self.chunk_size)
                        if not chunk:
                            break
                        self.limiter.acquire(len(chunk))
                        f.write(chunk)
                        downloaded += len(chunk)
                        tracker.add(len(chunk))
        except KeyboardInterrupt:
            tracker.emit()
            sys.stderr.write("\ninterrupted; re-run to resume\n")
            raise

        if total is not None and downloaded < total:
            tracker.emit()
            raise IntegrityError(f"incomplete download: {downloaded}/{total} bytes")

        _verify_size(part, total)
        verify_integrity(part, size=total, sha256=sha256, sha1=sha1, md5=md5)
        os.replace(part, dest)
        state_path.unlink(missing_ok=True)
        tracker.emit()
        return str(dest)


class SegmentManager:
    """Phase 4: parallel ranges, dynamic splitting, adaptive connections, crash-safe state."""

    def __init__(
        self,
        config: ClientConfig | None = None,
        progress: ProgressHook | None = default_progress_hook,
        chunk_size: int = 256 * 1024,
        speed_limit_bps: float | None = None,
    ) -> None:
        self.config = config or ClientConfig()
        self.progress = progress
        self.chunk_size = chunk_size
        self.limiter = SpeedLimiter(speed_limit_bps)

    def download(
        self,
        url: str,
        output: str | None = None,
        connections: int = 4,
        resume: bool = True,
        *,
        sha256: str | None = None,
        sha1: str | None = None,
        md5: str | None = None,
        output_dir: str | None = None,
        output_template: str | None = None,
        index: int = 1,
    ) -> str:
        connections = max(1, min(int(connections), 16))
        info = probe(url, self.config)
        total = info.get("size")
        accept_ranges = bool(info.get("accept_ranges"))
        if connections <= 1 or not total or not accept_ranges:
            return SingleStreamDownloader(
                self.config, self.progress, self.chunk_size,
                speed_limit_bps=self.limiter.rate,
            ).download(
                url, output, resume=resume, sha256=sha256, sha1=sha1, md5=md5,
                output_dir=output_dir, output_template=output_template, index=index,
            )

        named = expand_output(output_template, url=url, info=info, index=index, directory=output_dir)
        dest = Path(output or (str(named) if named else None) or info.get("filename") or "download").expanduser()
        if dest.exists() and dest.is_dir():
            dest = dest / (info.get("filename") or "download")

        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        state_path = dest.with_name(dest.name + ".mydl.json")
        target_url = info.get("url") or url

        state = _State(
            url=url, final_url=target_url, size=total,
            etag=info.get("etag"), last_modified=info.get("last_modified"),
            accept_ranges=True, mode="segments",
            sha256=(sha256.lower().strip() if sha256 else None),
        )
        existing = _load_state(state_path) if resume else None
        if existing and existing.url not in {url, state.final_url}:
            existing = None
        if existing and not _validators_match(state, existing):
            existing = None

        min_split = max(512 * 1024, self.chunk_size * 8)
        if existing and existing.mode == "segments" and existing.segments and existing.size == total and part.exists():
            segments = existing.segments
        else:
            if part.exists():
                part.unlink(missing_ok=True)
            segments = _split_segments(total, min(connections, max(2, connections // 2 or 1)), min_size=min_split)
            with open(part, "wb") as f:
                f.truncate(total)
            state.segments = segments
            _save_state(state_path, state)

        already = sum(max(0, min(s["done"], s["end"] + 1) - s["start"]) for s in segments)
        tracker = _ProgressTracker(total, self.progress)
        tracker.set_downloaded(already)
        state_lock = threading.Lock()
        stop = threading.Event()
        errors: list[BaseException] = []
        last_save = time.monotonic()

        def save(force: bool = False) -> None:
            nonlocal last_save
            now = time.monotonic()
            if not force and now - last_save < 0.75:
                return
            state.segments = [dict(s) for s in segments]
            _save_state(state_path, state)
            last_save = now

        def remaining(seg: dict[str, int]) -> int:
            return max(0, seg["end"] - seg["done"] + 1)

        def maybe_split() -> dict[str, int] | None:
            # Dynamic splitting: steal half of largest remaining segment when workers are idle.
            cand = [s for s in segments if remaining(s) >= min_split * 2]
            if not cand:
                return None
            cand.sort(key=remaining, reverse=True)
            src = cand[0]
            rem = remaining(src)
            half = rem // 2
            mid = src["done"] + half - 1
            if not (src["done"] <= mid < src["end"]):
                return None
            new_seg = {"start": mid + 1, "end": src["end"], "done": mid + 1}
            src["end"] = mid
            segments.append(new_seg)
            save(force=True)
            return new_seg

        def worker(seg: dict[str, int]) -> None:
            # One client and one file handle per worker (Windows-safe).
            client = HTTPClient(self.config)
            try:
                while not stop.is_set():
                    with state_lock:
                        start = seg["done"]; end = seg["end"]
                    if start > end:
                        return
                    headers = {"Range": f"bytes={start}-{end}"}
                    if state.etag:
                        headers["If-Range"] = state.etag
                    elif state.last_modified:
                        headers["If-Range"] = state.last_modified
                    with client.open(target_url, headers=headers) as resp:
                        status = getattr(resp, "status", 200)
                        if status == 200:
                            raise RuntimeError("server ignored range request during segmented download")
                        if status != 206:
                            raise urllib.error.HTTPError(target_url, status, "unexpected status", resp.headers, None)
                        with open(part, "r+b") as fh:
                            fh.seek(start)
                            expected = end - start + 1
                            got = 0
                            while got < expected and not stop.is_set():
                                chunk = resp.read(min(self.chunk_size, expected - got))
                                if not chunk:
                                    break
                                self.limiter.acquire(len(chunk))
                                fh.write(chunk)
                                n = len(chunk)
                                got += n
                                with state_lock:
                                    seg["done"] += n
                                    save()
                                tracker.add(n)
                            if got < expected and not stop.is_set():
                                raise IOError(f"short segment read: {got}/{expected}")
                    with state_lock:
                        save(force=True)
                    return
            except Exception as e:
                if not stop.is_set():
                    errors.append(e)
                    stop.set()
                raise

        def pending_segments() -> list[dict[str, int]]:
            with state_lock:
                return [s for s in segments if s["done"] <= s["end"]]

        try:
            # Adaptive start: begin with a modest worker count, allow growth by enqueuing split work.
            max_workers = max(1, connections)
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {}
                for seg in pending_segments():
                    if len(futures) >= max_workers:
                        break
                    futures[ex.submit(worker, seg)] = seg
                while futures and not stop.is_set():
                    done, _ = as_completed(list(futures.keys()), timeout=0.5), None
                    finished = [f for f in list(futures.keys()) if f.done()]
                    if not finished:
                        with state_lock:
                            save()
                        continue
                    for fut in finished:
                        futures.pop(fut, None)
                        try:
                            fut.result()
                        except Exception:
                            pass
                    if stop.is_set():
                        break
                    # Dynamic splitting: if we have idle capacity, split a large remaining segment.
                    with state_lock:
                        incomplete = [s for s in segments if s["done"] <= s["end"]]
                    while len(futures) < max_workers and incomplete:
                        with state_lock:
                            new_seg = maybe_split()
                        if new_seg is None:
                            # submit any untouched existing incomplete segment not already active
                            active = set(id(s) for s in futures.values())
                            candidate = None
                            with state_lock:
                                for s in segments:
                                    if s["done"] <= s["end"] and id(s) not in active:
                                        candidate = s; break
                            if candidate is None:
                                break
                            futures[ex.submit(worker, candidate)] = candidate
                        else:
                            futures[ex.submit(worker, new_seg)] = new_seg
                        with state_lock:
                            incomplete = [s for s in segments if s["done"] <= s["end"]]
                    with state_lock:
                        save()
        except KeyboardInterrupt:
            stop.set()
            with state_lock:
                save(force=True)
            tracker.emit()
            sys.stderr.write("\ninterrupted; re-run to resume\n")
            raise

        with state_lock:
            save(force=True)
            incomplete = [s for s in segments if s["done"] <= s["end"]]
        if errors:
            tracker.emit()
            raise errors[0]
        if incomplete:
            tracker.emit()
            raise IntegrityError("incomplete segmented download")

        final_size = part.stat().st_size if part.exists() else 0
        if final_size != total:
            raise IntegrityError(f"size mismatch: {final_size}/{total}")
        verify_integrity(part, size=total, sha256=sha256, sha1=sha1, md5=md5)
        os.replace(part, dest)
        state_path.unlink(missing_ok=True)
        tracker.emit()
        return str(dest)


# --- Mirror-aware download functions ---

def _make_single_downloader(config: ClientConfig, progress: ProgressHook | None,
                            chunk_size: int, speed_limit_bps: float | None) -> SingleStreamDownloader:
    return SingleStreamDownloader(config, progress, chunk_size, speed_limit_bps)


def _make_segment_downloader(config: ClientConfig, progress: ProgressHook | None,
                             chunk_size: int, speed_limit_bps: float | None) -> SegmentManager:
    return SegmentManager(config, progress, chunk_size, speed_limit_bps)


def download_with_mirrors(
    mirror_set: MirrorSet,
    *,
    config: ClientConfig | None = None,
    progress: ProgressHook | None = default_progress_hook,
    chunk_size: int = 256 * 1024,
    speed_limit_bps: float | None = None,
    connections: int = 1,
    resume: bool = True,
    sha256: str | None = None,
    sha1: str | None = None,
    md5: str | None = None,
    output: str | None = None,
    output_dir: str | None = None,
    output_template: str | None = None,
    index: int = 1,
) -> tuple[str, str]:  # (chosen_url, dest_path)
    """
    Download using mirror strategy.
    Returns (chosen_mirror_url, destination_path).
    """
    config = config or ClientConfig()

    if mirror_set.strategy == MirrorStrategy.FASTEST:
        # Probe all, pick best, then download with that URL
        best_url = mirror_set.select_best(config)
        if connections > 1:
            mgr = _make_segment_downloader(config, progress, chunk_size, speed_limit_bps)
            path = mgr.download(
                best_url, output, connections=connections, resume=resume,
                sha256=sha256, sha1=sha1, md5=md5,
                output_dir=output_dir, output_template=output_template, index=index,
            )
        else:
            dl = _make_single_downloader(config, progress, chunk_size, speed_limit_bps)
            path = dl.download(
                best_url, output, resume=resume,
                sha256=sha256, sha1=sha1, md5=md5,
                output_dir=output_dir, output_template=output_template, index=index,
            )
        return best_url, path

    elif mirror_set.strategy == MirrorStrategy.PARALLEL:
        # Race: start download on all mirrors, use first success
        def make_downloader(url: str):
            if connections > 1:
                return _make_segment_downloader(config, progress, chunk_size, speed_limit_bps)
            return _make_single_downloader(config, progress, chunk_size, speed_limit_bps)

        def download_fn(url: str):
            dl = make_downloader(url)
            if connections > 1:
                return dl.download(
                    url, output, connections=connections, resume=resume,
                    sha256=sha256, sha1=sha1, md5=md5,
                    output_dir=output_dir, output_template=output_template, index=index,
                )
            else:
                return dl.download(
                    url, output, resume=resume,
                    sha256=sha256, sha1=sha1, md5=md5,
                    output_dir=output_dir, output_template=output_template, index=index,
                )

        chosen_url, path = mirror_set.race_download(
            download_fn,
            max_workers=min(4, len(mirror_set.urls))
        )
        return chosen_url, path

    else:  # FALLBACK
        last_error: Exception | None = None
        for url in mirror_set.iter_candidates():
            try:
                if connections > 1:
                    mgr = _make_segment_downloader(config, progress, chunk_size, speed_limit_bps)
                    path = mgr.download(
                        url, output, connections=connections, resume=resume,
                        sha256=sha256, sha1=sha1, md5=md5,
                        output_dir=output_dir, output_template=output_template, index=index,
                    )
                else:
                    dl = _make_single_downloader(config, progress, chunk_size, speed_limit_bps)
                    path = dl.download(
                        url, output, resume=resume,
                        sha256=sha256, sha1=sha1, md5=md5,
                        output_dir=output_dir, output_template=output_template, index=index,
                    )
                return url, path
            except Exception as e:
                last_error = e
                if progress:
                    progress(Progress(downloaded=0, total=None, speed_bps=0, eta_seconds=None))
                continue
        raise RuntimeError(f"All mirrors failed: {last_error}")
