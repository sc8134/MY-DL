"""Enhanced FTP/FTPS support (stdlib ftplib + ssl).

Features:
- FTPS (explicit TLS) via AUTH TLS + PROT P
- Parallel range downloads (multiple data connections with REST)
- MLSD/MLST for reliable directory listings and metadata
- Passive (EPSV/PASV) and Active (EPRT/PORT) modes
- Robust resume with SIZE + MDTM + MLST validation
"""

from __future__ import annotations

import ftplib
import io
import re
import socket
import ssl
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator

from .ratelimit import SpeedLimiter


@dataclass
class FTPConfig:
    host: str
    port: int = 21
    user: str = "anonymous"
    passwd: str = "anonymous@"
    passive: bool = True
    ftps: bool = False
    tls_verify: bool = True
    ca_file: str | None = None
    timeout: float = 30.0
    encoding: str = "utf-8"


@dataclass
class FTPEntry:
    name: str
    path: str
    type: str  # "file" | "dir" | "link"
    size: int | None = None
    modified: datetime | None = None
    perm: str | None = None
    unix_user: str | None = None
    unix_group: str | None = None
    unique: str | None = None


@dataclass
class FTPFileInfo:
    url: str
    size: int | None
    modified: datetime | None
    type: str  # "file" | "dir"
    accept_ranges: bool = True  # FTP supports REST


class FTPError(Exception):
    pass


class FTPConnectionError(FTPError):
    pass


class FTPCommandError(FTPError):
    def __init__(self, cmd: str, response: str):
        self.cmd = cmd
        self.response = response
        super().__init__(f"FTP command {cmd!r} failed: {response}")


def _parse_ftp_url(url: str) -> tuple[str, int, str, str, str]:
    p = urllib.parse.urlparse(url)
    if p.scheme not in ("ftp", "ftps"):
        raise ValueError(f"Not an FTP URL: {url}")
    host = p.hostname or ""
    port = p.port or (990 if p.scheme == "ftps" else 21)
    user = urllib.parse.unquote(p.username or "anonymous")
    passwd = urllib.parse.unquote(p.password or "anonymous@")
    path = urllib.parse.unquote(p.path or "/")
    return host, port, user, passwd, path


def _create_ssl_context(tls_verify: bool, ca_file: str | None) -> ssl.SSLContext:
    if ca_file:
        ctx = ssl.create_default_context(cafile=ca_file)
    else:
        ctx = ssl.create_default_context()
    if not tls_verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


class FTPClient:
    """FTP/FTPS client with parallel download support."""

    def __init__(self, config: FTPConfig) -> None:
        self.config = config
        self.ctrl: ftplib.FTP | ftplib.FTP_TLS | None = None
        self._data_sock: socket.socket | None = None
        self._tls_context: ssl.SSLContext | None = None
        self._feat: dict[str, list[str]] = {}
        self._epsv_supported = False
        self._mlsd_supported = False

    def connect(self) -> None:
        host, port = self.config.host, self.config.port
        if self.config.ftps:
            self._tls_context = _create_ssl_context(self.config.tls_verify, self.config.ca_file)
            self.ctrl = ftplib.FTP_TLS(context=self._tls_context, timeout=self.config.timeout)
            self.ctrl.connect(host, port)
            self.ctrl.auth()
            self.ctrl.prot_p()  # Protect data channel
        else:
            self.ctrl = ftplib.FTP(timeout=self.config.timeout)
            self.ctrl.connect(host, port)
        self.ctrl.encoding = self.config.encoding
        self.ctrl.login(self.config.user, self.config.passwd)
        self.ctrl.voidcmd("TYPE I")
        self._probe_features()

    def _probe_features(self) -> None:
        assert self.ctrl is not None
        try:
            feat_lines = []
            self.ctrl.voidcmd("FEAT")
            # FEAT response is multi-line; ftplib doesn't expose it directly
            # Use low-level socket to read
            sock = self.ctrl.sock
            if sock:
                sock.settimeout(self.config.timeout)
                while True:
                    line = sock.readline(self.config.encoding).strip()
                    if line == "211 End":
                        break
                    if line and not line.startswith("211-"):
                        feat_lines.append(line)
            for line in feat_lines:
                parts = line.split()
                if parts:
                    self._feat[parts[0]] = parts[1:]
            self._epsv_supported = "EPSV" in self._feat
            self._mlsd_supported = "MLSD" in self._feat
        except Exception:
            pass

    def close(self) -> None:
        if self.ctrl:
            try:
                self.ctrl.quit()
            except Exception:
                try:
                    self.ctrl.close()
                except Exception:
                    pass
            self.ctrl = None

    def __enter__(self) -> "FTPClient":
        self.connect()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # --- Low-level data connection handling ---

    def _make_data_connection(self, cmd: str) -> socket.socket | ssl.SSLSocket:
        """Create a data connection (passive or active) and send the transfer command."""
        assert self.ctrl is not None
        if self.config.passive:
            return self._passive_data_connection(cmd)
        return self._active_data_connection(cmd)

    def _passive_data_connection(self, cmd: str) -> socket.socket | ssl.SSLSocket:
        assert self.ctrl is not None
        if self._epsv_supported:
            resp = self.ctrl.sendcmd("EPSV")
            # Parse EPSV response: 229 Entering Extended Passive Mode (|||port|)
            match = re.search(r"\(\|\|(\d+)\|\)", resp)
            if not match:
                raise FTPCommandError("EPSV", resp)
            port = int(match.group(1))
            host = self.ctrl.sock.getpeername()[0] if self.ctrl.sock else self.config.host
        else:
            resp = self.ctrl.sendcmd("PASV")
            # Parse PASV response: 227 Entering Passive Mode (h1,h2,h3,h4,p1,p2)
            match = re.search(r"\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)", resp)
            if not match:
                raise FTPCommandError("PASV", resp)
            h1, h2, h3, h4, p1, p2 = map(int, match.groups())
            host = f"{h1}.{h2}.{h3}.{h4}"
            port = (p1 << 8) + p2

        data_sock = socket.create_connection((host, port), timeout=self.config.timeout)
        if self.config.ftps and self._tls_context:
            data_sock = self._tls_context.wrap_socket(data_sock, server_hostname=self.config.host)
        self.ctrl.sendcmd(cmd)
        return data_sock

    def _active_data_connection(self, cmd: str) -> socket.socket | ssl.SSLSocket:
        assert self.ctrl is not None
        # Get local IP for PORT/EPRT
        local_ip = self.ctrl.sock.getsockname()[0] if self.ctrl.sock else "0.0.0.0"
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((local_ip, 0))
        server_sock.listen(1)
        port = server_sock.getsockname()[1]

        if ":" in local_ip or hasattr(socket, "AF_INET6"):
            # IPv6 - use EPRT
            self.ctrl.sendcmd(f"EPRT |2|{local_ip}|{port}|")
        else:
            # IPv4 - use PORT
            parts = local_ip.split(".") + [str(port >> 8), str(port & 0xFF)]
            self.ctrl.sendcmd(f"PORT {','.join(parts)}")

        self.ctrl.sendcmd(cmd)
        data_sock, _ = server_sock.accept()
        data_sock.settimeout(self.config.timeout)
        server_sock.close()
        if self.config.ftps and self._tls_context:
            data_sock = self._tls_context.wrap_socket(data_sock, server_hostname=self.config.host)
        return data_sock

    # --- High-level operations ---

    def size(self, path: str) -> int | None:
        assert self.ctrl is not None
        try:
            resp = self.ctrl.sendcmd(f"SIZE {path}")
            if resp.startswith("213"):
                return int(resp.split()[1])
        except ftplib.error_perm:
            pass
        return None

    def mdtm(self, path: str) -> datetime | None:
        assert self.ctrl is not None
        try:
            resp = self.ctrl.sendcmd(f"MDTM {path}")
            if resp.startswith("213"):
                # Format: YYYYMMDDHHMMSS[.sss] or YYYYMMDDHHMMSS
                ts = resp.split()[1]
                for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M%S.%f"):
                    try:
                        return datetime.strptime(ts, fmt)
                    except ValueError:
                        continue
        except ftplib.error_perm:
            pass
        return None

    def mlst(self, path: str) -> dict[str, str] | None:
        """Get MLST facts for a single path."""
        assert self.ctrl is not None
        if not self._mlsd_supported:
            return None
        try:
            resp = self.ctrl.sendcmd(f"MLST {path}")
            if resp.startswith("250-"):
                # Multi-line response
                lines = [resp[4:].strip()]
                while True:
                    line = self.ctrl.sock.recv(4096).decode(self.config.encoding).strip()
                    if line.startswith("250 "):
                        break
                    lines.append(line)
                facts_str = "".join(lines).strip()
            else:
                facts_str = resp[4:].strip()
            return self._parse_mlst_facts(facts_str)
        except ftplib.error_perm:
            pass
        return None

    def _parse_mlst_facts(self, facts: str) -> dict[str, str]:
        """Parse MLST/MLSD fact string: key=val;key=val; filename"""
        facts_part, _, name = facts.rpartition(" ")
        result: dict[str, str] = {"name": name}
        for fact in facts_part.split(";"):
            if not fact:
                continue
            if "=" in fact:
                k, v = fact.split("=", 1)
                result[k.lower()] = v
        return result

    def list_dir(self, path: str = "/") -> list[FTPEntry]:
        """List directory using MLSD (preferred) or fallback to LIST."""
        assert self.ctrl is not None
        if self._mlsd_supported:
            return self._list_dir_mlsd(path)
        return self._list_dir_list(path)

    def _list_dir_mlsd(self, path: str) -> list[FTPEntry]:
        assert self.ctrl is not None
        entries: list[FTPEntry] = []
        # Use retrlines to capture MLSD output
        lines: list[str] = []
        self.ctrl.retrlines(f"MLSD {path}", lines.append)
        for line in lines:
            facts = self._parse_mlst_facts(line)
            entry = FTPEntry(
                name=facts.get("name", ""),
                path=path.rstrip("/") + "/" + facts.get("name", ""),
                type=facts.get("type", "file"),
                size=int(facts["size"]) if "size" in facts and facts["size"].isdigit() else None,
                modified=self._parse_mlst_time(facts.get("modify")),
                perm=facts.get("perm"),
                unix_user=facts.get("unix.owner"),
                unix_group=facts.get("unix.group"),
                unique=facts.get("unique"),
            )
            entries.append(entry)
        return entries

    def _parse_mlst_time(self, s: str | None) -> datetime | None:
        if not s:
            return None
        for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M%S.%f"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    def _list_dir_list(self, path: str) -> list[FTPEntry]:
        """Fallback directory listing using LIST command (Unix-style parsing)."""
        assert self.ctrl is not None
        entries: list[FTPEntry] = []
        lines: list[str] = []
        self.ctrl.retrlines(f"LIST {path}", lines.append)
        # Simple Unix-style ls -l parsing
        for line in lines:
            if not line or line.startswith("total"):
                continue
            # drwxr-xr-x 2 user group 4096 Jan 1 12:00 name
            parts = line.split(maxsplit=8)
            if len(parts) < 9:
                continue
            perm, _, _, _, size_str, month, day, time_or_year, name = parts
            entry_type = "dir" if perm.startswith("d") else ("link" if perm.startswith("l") else "file")
            try:
                size = int(size_str)
            except ValueError:
                size = None
            # Date parsing is best-effort
            modified = None
            entries.append(FTPEntry(
                name=name,
                path=path.rstrip("/") + "/" + name,
                type=entry_type,
                size=size,
                modified=modified,
                perm=perm,
            ))
        return entries

    def download_range(
        self,
        path: str,
        start: int,
        end: int,
        dest_fh: io.BufferedWriter,
        *,
        limiter: SpeedLimiter | None = None,
        progress: Callable[[int], None] | None = None,
    ) -> int:
        """Download byte range [start, end] inclusive to an already-seeked file handle."""
        assert self.ctrl is not None
        expected = end - start + 1
        if expected <= 0:
            return 0

        # Send REST command
        rest_resp = self.ctrl.sendcmd(f"REST {start}")
        if not rest_resp.startswith(("350", "200")):
            raise FTPCommandError(f"REST {start}", rest_resp)

        data_sock = self._make_data_connection(f"RETR {path}")
        try:
            # Wrap socket in a file-like object for efficient reading
            with data_sock.makefile("rb", buffering=65536) as rf:
                got = 0
                while got < expected:
                    chunk = rf.read(min(256 * 1024, expected - got))
                    if not chunk:
                        break
                    if limiter:
                        limiter.acquire(len(chunk))
                    dest_fh.write(chunk)
                    got += len(chunk)
                    if progress:
                        progress(got)
                if got < expected:
                    raise FTPError(f"Short read: got {got}/{expected} bytes")
            # Consume the final response
            self.ctrl.voidresp()
            return got
        finally:
            data_sock.close()

    def download_full(
        self,
        path: str,
        dest: Path,
        *,
        resume: bool = True,
        chunk_size: int = 256 * 1024,
        progress: Callable[[int, int | None], None] | None = None,
        limiter: SpeedLimiter | None = None,
    ) -> None:
        """Single-stream download with resume support."""
        info = self.probe(path)
        total = info.size
        part = dest.with_name(dest.name + ".part")
        dest.parent.mkdir(parents=True, exist_ok=True)

        start = 0
        mode = "wb"
        if resume and part.exists():
            start = part.stat().st_size
            if total is not None and start >= total > 0:
                # Already complete
                if progress:
                    try:
                        progress(start, total)
                    except TypeError:
                        from .downloader import Progress
                        progress(Progress(downloaded=start, total=total))
                part.replace(dest)
                return
            mode = "ab" if start > 0 else "wb"
        elif part.exists():
            part.unlink(missing_ok=True)

        downloaded = start
        with open(part, mode) as f:
            if start > 0:
                try:
                    self.ctrl.sendcmd(f"REST {start}")
                except ftplib.error_perm:
                    # Server doesn't support REST, restart
                    start = 0
                    downloaded = 0
                    f.close()
                    part.unlink(missing_ok=True)
                    with open(part, "wb") as f2:
                        f = f2  # type: ignore

            data_sock = self._make_data_connection(f"RETR {path}")
            try:
                with data_sock.makefile("rb", buffering=chunk_size) as rf:
                    while True:
                        chunk = rf.read(chunk_size)
                        if not chunk:
                            break
                        if limiter:
                            limiter.acquire(len(chunk))
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress:
                            try:
                                progress(downloaded, total)
                            except TypeError:
                                from .downloader import Progress
                                progress(Progress(downloaded=downloaded, total=total))
                self.ctrl.voidresp()
            finally:
                data_sock.close()

        if total is not None and downloaded < total:
            raise FTPError(f"Incomplete download: {downloaded}/{total} bytes")
        part.replace(dest)

    def probe(self, path: str) -> FTPFileInfo:
        """Get file metadata (size, modified, type)."""
        size = self.size(path)
        modified = self.mdtm(path)
        entry_type = "file"
        if size is None and modified is None:
            # Might be a directory; try MLST
            mlst = self.mlst(path)
            if mlst:
                entry_type = mlst.get("type", "file")
                if "size" in mlst and mlst["size"].isdigit():
                    size = int(mlst["size"])
                if "modify" in mlst:
                    modified = self._parse_mlst_time(mlst["modify"])
        return FTPFileInfo(
            url=f"ftp://{self.config.host}{path}",
            size=size,
            modified=modified,
            type=entry_type,
            accept_ranges=True,
        )


def probe_ftp(url: str, timeout: float = 30.0, ftps: bool = False, tls_verify: bool = True) -> dict[str, Any]:
    """Probe FTP URL for metadata (compatible with HTTP probe interface)."""
    host, port, user, passwd, path = _parse_ftp_url(url)
    config = FTPConfig(host=host, port=port, user=user, passwd=passwd, ftps=ftps, tls_verify=tls_verify, timeout=timeout)
    with FTPClient(config) as client:
        info = client.probe(path)
        filename = Path(path).name or "download"
        return {
            "url": url,
            "status": 200,
            "size": info.size,
            "accept_ranges": info.accept_ranges,
            "content_type": "application/octet-stream" if info.type == "file" else "inode/directory",
            "etag": None,
            "last_modified": info.modified.isoformat() if info.modified else None,
            "filename": filename,
        }


def download_ftp(
    url: str,
    dest: Path,
    *,
    resume: bool = True,
    timeout: float = 30.0,
    chunk_size: int = 256 * 1024,
    progress: Callable[[int, int | None], None] | None = None,
    limiter: SpeedLimiter | None = None,
    ftps: bool = False,
    tls_verify: bool = True,
    passive: bool = True,
) -> None:
    """Backward-compatible single-stream FTP download."""
    host, port, user, passwd, path = _parse_ftp_url(url)
    config = FTPConfig(host=host, port=port, user=user, passwd=passwd,
                       ftps=ftps, tls_verify=tls_verify, passive=passive, timeout=timeout)
    with FTPClient(config) as client:
        client.download_full(path, dest, resume=resume, chunk_size=chunk_size,
                             progress=progress, limiter=limiter)


# --- Parallel FTP Download Manager ---

@dataclass
class _FTPSegmentState:
    start: int
    end: int
    done: int


class FTPSegmentManager:
    """Parallel range downloads over FTP (like SegmentManager for HTTP)."""

    def __init__(
        self,
        config: FTPConfig,
        progress: Callable[[int, int | None], None] | None = None,
        chunk_size: int = 256 * 1024,
        speed_limit_bps: float | None = None,
    ) -> None:
        self.config = config
        self.progress = progress
        self.chunk_size = chunk_size
        self.limiter = SpeedLimiter(speed_limit_bps)
        self._client: FTPClient | None = None

    def __enter__(self) -> "FTPSegmentManager":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client:
            self._client.close()
            self._client = None

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
        from .downloader import IntegrityError, verify_integrity
        from .naming import expand_output

        connections = max(1, min(int(connections), 16))
        host, port, user, passwd, path = _parse_ftp_url(url)
        self.config.host = host
        self.config.port = port
        self.config.user = user
        self.config.passwd = passwd

        with FTPClient(self.config) as client:
            info = client.probe(path)
            total = info.size
            accept_ranges = info.accept_ranges
            if connections <= 1 or not total or not accept_ranges:
                # Fallback to single-stream
                return self._single_stream_download(client, path, url, output, resume, sha256, sha1, md5,
                                                   output_dir, output_template, index)

            named = expand_output(output_template, url=url, info={"size": total, "url": url, "filename": Path(path).name},
                                  index=index, directory=output_dir)
            dest = Path(output or (str(named) if named else None) or Path(path).name).expanduser()
            if dest.exists() and dest.is_dir():
                dest = dest / Path(path).name

            dest.parent.mkdir(parents=True, exist_ok=True)
            part = dest.with_name(dest.name + ".part")
            state_path = dest.with_name(dest.name + ".mydl.json")

            # Load or create segments
            segments = self._load_or_create_segments(client, part, state_path, total, connections, resume)

            # Download segments in parallel with fallback to single-stream on connection limit errors
            try:
                self._download_segments_parallel(client, path, part, segments, total)
            except ftplib.error_temp as e:
                if "421" in str(e) or "426" in str(e) or "limit" in str(e).lower():
                    # Connection limit hit - fallback to single-stream
                    if not self.progress:
                        print("FTP server limits concurrent connections; falling back to single-stream", file=sys.stderr)
                    # Resume from where we left off using single-stream
                    return self._single_stream_download(client, path, url, output, resume, sha256, sha1, md5,
                                                       output_dir, output_template, index)
                raise

            # Verify and finalize
            final_size = part.stat().st_size if part.exists() else 0
            if final_size != total:
                raise IntegrityError(f"size mismatch: {final_size}/{total}")
            verify_integrity(part, size=total, sha256=sha256, sha1=sha1, md5=md5)
            os.replace(part, dest)
            state_path.unlink(missing_ok=True)
            return str(dest)

    def _single_stream_download(
        self,
        client: FTPClient,
        path: str,
        url: str,
        output: str | None,
        resume: bool,
        sha256: str | None,
        sha1: str | None,
        md5: str | None,
        output_dir: str | None,
        output_template: str | None,
        index: int,
    ) -> str:
        from .downloader import IntegrityError, verify_integrity
        from .naming import expand_output

        info = client.probe(path)
        named = expand_output(output_template, url=url, info={"size": info.size, "url": url, "filename": Path(path).name},
                              index=index, directory=output_dir)
        dest = Path(output or (str(named) if named else None) or Path(path).name).expanduser()
        if dest.exists() and dest.is_dir():
            dest = dest / Path(path).name

        client.download_full(path, dest, resume=resume, chunk_size=self.chunk_size,
                             progress=self.progress, limiter=self.limiter)

        from .downloader import verify_integrity as vi
        vi(dest, size=info.size, sha256=sha256, sha1=sha1, md5=md5)
        return str(dest)

    def _load_or_create_segments(
        self,
        client: FTPClient,
        part: Path,
        state_path: Path,
        total: int,
        connections: int,
        resume: bool,
    ) -> list[_FTPSegmentState]:
        import json
        import os

        min_split = max(512 * 1024, self.chunk_size * 8)
        n = max(1, connections)
        if total // n < min_split:
            n = max(1, total // min_split)
        n = max(1, min(n, connections, total if total > 0 else 1))
        size = total // n

        if resume and state_path.exists() and part.exists():
            try:
                data = json.loads(state_path.read_text(encoding="utf-8"))
                if data.get("size") == total and data.get("mode") == "ftp_segments":
                    segs_data = data.get("segments", [])
                    segments = [_FTPSegmentState(**s) for s in segs_data]
                    # Verify part file size
                    if part.stat().st_size == total:
                        return segments
            except Exception:
                pass

        # Create new segments
        if part.exists():
            part.unlink(missing_ok=True)
        segments = []
        start = 0
        for i in range(n):
            end = (start + size - 1) if i < n - 1 else (total - 1)
            segments.append(_FTPSegmentState(start=start, end=end, done=start))
            start = end + 1

        with open(part, "wb") as f:
            f.truncate(total)
        self._save_state(state_path, total, segments)
        return segments

    def _save_state(self, state_path: Path, total: int, segments: list[_FTPSegmentState]) -> None:
        import json
        import os
        state_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "size": total,
            "mode": "ftp_segments",
            "segments": [{"start": s.start, "end": s.end, "done": s.done} for s in segments],
        }
        tmp = state_path.with_name(state_path.name + ".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        os.replace(tmp, state_path)

    def _download_segments_parallel(
        self,
        client: FTPClient,
        path: str,
        part: Path,
        segments: list[_FTPSegmentState],
        total: int,
    ) -> None:
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed

        max_workers = len(segments)
        tracker = _FTPProgressTracker(total, self.progress)
        already = sum(max(0, s.done - s.start) for s in segments)
        tracker.set_downloaded(already)

        state_lock = threading.Lock()
        stop = threading.Event()
        errors: list[BaseException] = []
        last_save = 0.0

        def save_state(force: bool = False) -> None:
            nonlocal last_save
            import time
            now = time.monotonic()
            if not force and now - last_save < 0.75:
                return
            self._save_state(part.with_name(part.name + ".mydl.json"), total, segments)
            last_save = now

        def worker(seg: _FTPSegmentState) -> None:
            import time
            # Each worker gets its own FTP connection (required for parallel data connections)
            worker_client = FTPClient(self.config)
            worker_client.connect()
            try:
                while not stop.is_set():
                    with state_lock:
                        start = seg.done
                        end = seg.end
                    if start > end:
                        return
                    with open(part, "r+b") as fh:
                        fh.seek(start)
                        expected = end - start + 1
                        got = 0
                        while got < expected and not stop.is_set():
                            chunk_size = min(self.chunk_size, expected - got)
                            # Use a small buffer read
                            data_sock = worker_client._make_data_connection(f"RETR {path}")
                            try:
                                with data_sock.makefile("rb", buffering=chunk_size) as rf:
                                    chunk = rf.read(chunk_size)
                                    if not chunk:
                                        break
                                    if self.limiter:
                                        self.limiter.acquire(len(chunk))
                                    fh.write(chunk)
                                    n = len(chunk)
                                    got += n
                                    with state_lock:
                                        seg.done += n
                                        save_state()
                                    tracker.add(n)
                            finally:
                                data_sock.close()
                        if got < expected and not stop.is_set():
                            raise FTPError(f"short segment read: {got}/{expected}")
                    with state_lock:
                        save_state(force=True)
                    return
            except Exception as e:
                if not stop.is_set():
                    errors.append(e)
                    stop.set()
                raise
            finally:
                worker_client.close()

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {ex.submit(worker, seg): seg for seg in segments}
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except Exception:
                        pass
                    if stop.is_set():
                        break
        except KeyboardInterrupt:
            stop.set()
            save_state(force=True)
            tracker.emit()
            raise

        if errors:
            tracker.emit()
            raise errors[0]
        incomplete = [s for s in segments if s.done <= s.end]
        if incomplete:
            tracker.emit()
            raise FTPError("incomplete segmented download")


class _FTPProgressTracker:
    def __init__(self, total: int | None, hook: Callable | None):
        self.total = total
        self.hook = hook
        self.lock = threading.Lock()
        self.downloaded = 0

    def add(self, n: int) -> None:
        if n <= 0:
            return
        with self.lock:
            self.downloaded += n
            self._emit_locked()

    def set_downloaded(self, n: int) -> None:
        with self.lock:
            self.downloaded = n
            self._emit_locked()

    def emit(self) -> None:
        with self.lock:
            self._emit_locked()

    def _emit_locked(self) -> None:
        if self.hook:
            try:
                self.hook(self.downloaded, self.total)
            except TypeError:
                from .downloader import Progress
                self.hook(Progress(downloaded=self.downloaded, total=self.total))


# Need to import threading and os at module level
import threading
import os