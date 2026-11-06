"""Download queue manager for web UI."""

from __future__ import annotations

import asyncio
import json
import uuid
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor

from ..downloader import IntegrityError, default_progress_hook, download_with_mirrors
from ..mirrors import MirrorSet, MirrorStrategy, build_mirror_set
from ..http import ClientConfig
from ..naming import expand_output
from ..ratelimit import parse_speed, SpeedLimiter
from ..ftp import probe_ftp, download_ftp, FTPSegmentManager, FTPConfig


class TaskStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class DownloadTask:
    id: str
    url: str
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    # Options
    mirrors: List[str] = field(default_factory=list)
    mirror_strategy: str = "fallback"
    mirror_probe_timeout: float = 5.0
    connections: int = 1
    speed_limit: Optional[str] = None
    resume: bool = True
    sha256: Optional[str] = None
    sha1: Optional[str] = None
    md5: Optional[str] = None
    output: Optional[str] = None
    output_dir: Optional[str] = None
    output_template: Optional[str] = None
    index: int = 1
    # FTP options
    ftp_passive: bool = True
    ftps: bool = False
    ftp_tls_verify: bool = True

    # Runtime state
    progress_downloaded: int = 0
    progress_total: Optional[int] = None
    progress_speed: float = 0.0
    progress_eta: Optional[float] = None
    error: Optional[str] = None
    result_path: Optional[str] = None
    chosen_mirror: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "url": self.url,
            "status": self.status.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "mirrors": self.mirrors,
            "mirror_strategy": self.mirror_strategy,
            "mirror_probe_timeout": self.mirror_probe_timeout,
            "connections": self.connections,
            "speed_limit": self.speed_limit,
            "resume": self.resume,
            "sha256": self.sha256,
            "sha1": self.sha1,
            "md5": self.md5,
            "output": self.output,
            "output_dir": self.output_dir,
            "output_template": self.output_template,
            "index": self.index,
            "ftp_passive": self.ftp_passive,
            "ftps": self.ftps,
            "ftp_tls_verify": self.ftp_tls_verify,
            "progress": {
                "downloaded": self.progress_downloaded,
                "total": self.progress_total,
                "speed": self.progress_speed,
                "eta": self.progress_eta,
            },
            "error": self.error,
            "result_path": self.result_path,
            "chosen_mirror": self.chosen_mirror,
        }


class DownloadQueue:
    def __init__(self, max_concurrent: int = 3):
        self.max_concurrent = max_concurrent
        self.tasks: Dict[str, DownloadTask] = {}
        self._running: Set[str] = set()
        self._paused: Set[str] = set()
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._subscribers: List[asyncio.Queue] = []
        self._lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

    def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue()
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        if q in self._subscribers:
            self._subscribers.remove(q)

    async def _notify(self, task: DownloadTask) -> None:
        data = task.to_dict()
        for q in self._subscribers[:]:
            try:
                q.put_nowait(data)
            except asyncio.QueueFull:
                pass

    def add_task(self, task: DownloadTask) -> str:
        self.tasks[task.id] = task
        return task.id

    def get_task(self, task_id: str) -> Optional[DownloadTask]:
        return self.tasks.get(task_id)

    def list_tasks(self) -> List[DownloadTask]:
        return list(self.tasks.values())

    async def start_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if not task or task.status not in (TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.PAUSED, TaskStatus.FAILED):
            return False

        async with self._lock:
            if len(self._running) >= self.max_concurrent:
                task.status = TaskStatus.QUEUED
                await self._notify(task)
                return True

            self._running.add(task_id)
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            task.error = None
            asyncio.create_task(self._run_task(task_id))
            await self._notify(task)
            return True

    async def pause_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if not task or task.status != TaskStatus.RUNNING:
            return False
        self._paused.add(task_id)
        task.status = TaskStatus.PAUSED
        await self._notify(task)
        return True

    async def resume_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if not task or task.status != TaskStatus.PAUSED:
            return False
        self._paused.discard(task_id)
        return await self.start_task(task_id)

    async def cancel_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if not task:
            return False
        if task.status == TaskStatus.RUNNING:
            self._paused.add(task_id)  # Signal worker to stop
        task.status = TaskStatus.CANCELLED
        task.completed_at = time.time()
        await self._notify(task)
        return True

    async def remove_task(self, task_id: str) -> bool:
        task = self.tasks.get(task_id)
        if not task:
            return False
        if task.status == TaskStatus.RUNNING:
            await self.cancel_task(task_id)
        del self.tasks[task_id]
        return True

    async def _run_task(self, task_id: str) -> None:
        task = self.tasks.get(task_id)
        if not task:
            return

        def progress_hook(progress):
            task.progress_downloaded = progress.downloaded
            task.progress_total = progress.total
            task.progress_speed = progress.speed_bps
            task.progress_eta = progress.eta_seconds

        try:
            # Determine if FTP
            is_ftp = task.url.lower().startswith("ftp://") or task.url.lower().startswith("ftps://")

            if is_ftp:
                await self._run_ftp_task(task, progress_hook)
            else:
                await self._run_http_task(task, progress_hook)

            task.status = TaskStatus.COMPLETED
            task.completed_at = time.time()
            await self._notify(task)
        except asyncio.CancelledError:
            task.status = TaskStatus.CANCELLED
            task.completed_at = time.time()
            await self._notify(task)
        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            task.completed_at = time.time()
            await self._notify(task)
        finally:
            self._running.discard(task_id)
            self._paused.discard(task_id)
            # Start next queued task
            await self._start_next_queued()

    async def _start_next_queued(self) -> None:
        async with self._lock:
            if len(self._running) >= self.max_concurrent:
                return
            for t in self.tasks.values():
                if t.status == TaskStatus.QUEUED:
                    self._running.add(t.id)
                    t.status = TaskStatus.RUNNING
                    t.started_at = time.time()
                    asyncio.create_task(self._run_task(t.id))
                    await self._notify(t)
                    break

    async def _run_http_task(self, task: DownloadTask, progress_hook) -> None:
        client_cfg = ClientConfig(
            timeout=30.0,
            retries=3,
            backoff=1.0,
            max_redirects=10,
            headers={},
            insecure=False,
            ca_file=None,
        )
        speed_limit_bps = parse_speed(task.speed_limit) if task.speed_limit else None
        limiter = SpeedLimiter(speed_limit_bps) if speed_limit_bps else None

        mirror_set = build_mirror_set(
            primary_url=task.url,
            mirror_args=task.mirrors,
            mirror_file=None,
            strategy=MirrorStrategy(task.mirror_strategy),
            probe_timeout=task.mirror_probe_timeout,
        )

        def check_paused():
            if task.id in self._paused:
                raise asyncio.CancelledError()

        # Wrap progress hook to check for pause
        def wrapped_hook(p):
            check_paused()
            progress_hook(p)

        # Run in thread pool
        loop = asyncio.get_event_loop()
        chosen_url, path = await loop.run_in_executor(
            self._executor,
            lambda: download_with_mirrors(
                mirror_set,
                config=client_cfg,
                progress=wrapped_hook,
                speed_limit_bps=speed_limit_bps,
                connections=task.connections,
                resume=task.resume,
                sha256=task.sha256,
                sha1=task.sha1,
                md5=task.md5,
                output=task.output,
                output_dir=task.output_dir,
                output_template=task.output_template,
                index=task.index,
            )
        )
        task.result_path = path
        task.chosen_mirror = chosen_url

    async def _run_ftp_task(self, task: DownloadTask, progress_hook) -> None:
        import urllib.parse
        p = urllib.parse.urlparse(task.url)
        host = p.hostname or ""
        port = p.port or (990 if p.scheme == "ftps" else 21)
        user = urllib.parse.unquote(p.username or "anonymous")
        passwd = urllib.parse.unquote(p.password or "anonymous@")
        path = urllib.parse.unquote(p.path or "/")

        ftp_cfg = FTPConfig(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            passive=task.ftp_passive,
            ftps=task.ftps or (p.scheme == "ftps"),
            tls_verify=task.ftp_tls_verify,
            ca_file=None,
            timeout=30.0,
        )

        speed_limit_bps = parse_speed(task.speed_limit) if task.speed_limit else None
        limiter = SpeedLimiter(speed_limit_bps) if speed_limit_bps else None

        def check_paused():
            if task.id in self._paused:
                raise asyncio.CancelledError()

        def wrapped_hook(downloaded, total):
            check_paused()
            from ..downloader import Progress
            progress_hook(Progress(downloaded=downloaded, total=total, speed_bps=0, eta_seconds=None))

        loop = asyncio.get_event_loop()

        if task.connections > 1:
            async def run_parallel():
                with FTPSegmentManager(ftp_cfg, progress=wrapped_hook, speed_limit_bps=speed_limit_bps) as mgr:
                    return mgr.download(
                        task.url, task.output,
                        connections=task.connections, resume=task.resume,
                        sha256=task.sha256, sha1=task.sha1, md5=task.md5,
                        output_dir=task.output_dir, output_template=task.output_template, index=task.index,
                    )
            path = await loop.run_in_executor(self._executor, run_parallel)
        else:
            info = probe_ftp(task.url, timeout=ftp_cfg.timeout, ftps=ftp_cfg.ftps, tls_verify=ftp_cfg.tls_verify)
            named = expand_output(task.output_template, url=task.url, info=info, index=task.index, directory=task.output_dir)
            dest = Path(task.output or (str(named) if named else None) or info.get("filename") or "download").expanduser()
            if dest.exists() and dest.is_dir():
                dest = dest / (info.get("filename") or "download")

            def run_single():
                download_ftp(
                    task.url, dest,
                    resume=task.resume,
                    timeout=ftp_cfg.timeout,
                    chunk_size=256 * 1024,
                    progress=wrapped_hook,
                    limiter=limiter,
                    ftps=ftp_cfg.ftps,
                    tls_verify=ftp_cfg.tls_verify,
                    passive=ftp_cfg.passive,
                )
                from ..downloader import verify_integrity
                verify_integrity(dest, size=info.get("size"), sha256=task.sha256, sha1=task.sha1, md5=task.md5)
                return str(dest)

            path = await loop.run_in_executor(self._executor, run_single)

        task.result_path = path

    async def shutdown(self) -> None:
        self._stop_event.set()
        for task_id in list(self._running):
            await self.cancel_task(task_id)
        # Wait for running tasks to finish
        while self._running:
            await asyncio.sleep(0.1)
        self._executor.shutdown(wait=True)