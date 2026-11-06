from __future__ import annotations

import argparse
import sys
import urllib.error
from pathlib import Path

from . import __version__
from .config import load_config, merged_get_options
from .downloader import IntegrityError, SegmentManager, SingleStreamDownloader, default_progress_hook, download_with_mirrors
from .mirrors import MirrorStrategy, build_mirror_set
from .ratelimit import parse_speed


def _add_client_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--timeout", type=float, default=None)
    p.add_argument("--retries", type=int, default=None)
    p.add_argument("--backoff", type=float, default=None)
    p.add_argument("--max-redirects", type=int, default=None)
    p.add_argument("-H", "--header", action="append", default=None, metavar="K:V")
    p.add_argument("-k", "--insecure", action="store_true", default=None, help="Disable TLS verification")
    p.add_argument("--ca-file")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="mydl", description="Stdlib download manager")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument("--config", help="Path to config file (.json or .toml)")
    sub = p.add_subparsers(dest="command")

    head = sub.add_parser("head", help="Probe URL metadata (Phase 1)")
    head.add_argument("url")
    _add_client_args(head)

    dl = sub.add_parser("get", help="Download URL(s) with queue/resume/integrity")

    web = sub.add_parser("web", help="Start web UI (Phase 7)")
    web.add_argument("--host", default=None, help="Bind address")
    web.add_argument("--port", type=int, default=None, help="Port")
    web.add_argument("--static-dir", help="Static files directory")
    web.add_argument("--max-concurrent", type=int, default=None, help="Max concurrent downloads")
    dl.add_argument("urls", nargs="*", help="URL(s) to download")
    dl.add_argument("-i", "--input-file", help="File with URLs (one per line; # comments)")
    dl.add_argument("-o", "--output", help="Output file path (single URL only)")
    dl.add_argument("-d", "--dir", dest="output_dir", help="Output directory")
    dl.add_argument("-O", "--name-template", help="Output naming template: {filename},{name},{ext},{index},{host}")
    dl.add_argument("-n", "--connections", type=int, default=None, help="Parallel connections per file")
    dl.add_argument("--limit-rate", dest="speed_limit", help="Global-ish per-stream speed limit (e.g. 512K, 2M)")
    dl.add_argument("--no-resume", action="store_true", default=None, help="Ignore existing .part/state")
    dl.add_argument("-q", "--quiet", action="store_true", default=None, help="Suppress progress output")
    dl.add_argument("--continue-on-error", action="store_true", default=None)
    dl.add_argument("--sha256", metavar="HASH", help="Verify SHA-256 (single URL only)")
    dl.add_argument("--sha1", metavar="HASH", help="Verify SHA-1 (single URL only)")
    dl.add_argument("--md5", metavar="HASH", help="Verify MD5 (single URL only)")

    # Mirror options
    dl.add_argument("--mirror", action="append", default=None, metavar="URL", help="Mirror URL (repeatable)")
    dl.add_argument("--mirror-file", help="File with mirror URLs (one per line; # comments)")
    dl.add_argument("--mirror-strategy", choices=["fallback", "parallel", "fastest"], default=None,
                    help="Mirror strategy: fallback (sequential), parallel (race), fastest (probe first)")
    dl.add_argument("--mirror-probe-timeout", type=float, default=None, help="Probe timeout for fastest strategy (seconds)")

    # FTP options
    dl.add_argument("--ftp-passive", action="store_true", default=None, help="Use passive mode (default)")
    dl.add_argument("--ftp-active", action="store_true", default=None, help="Use active mode")
    dl.add_argument("--ftps", action="store_true", default=None, help="Use FTPS (explicit TLS)")
    dl.add_argument("--ftp-tls-verify", action="store_true", default=None, help="Verify TLS certificate (default)")
    dl.add_argument("--ftp-no-tls-verify", action="store_true", default=None, help="Disable TLS verification")

    _add_client_args(dl)

    ver = sub.add_parser("verify", help="Verify integrity of a local file")
    ver.add_argument("path")
    ver.add_argument("--size", type=int)
    ver.add_argument("--sha256", metavar="HASH")
    ver.add_argument("--sha1", metavar="HASH")
    ver.add_argument("--md5", metavar="HASH")
    return p


def _parse_headers(items: list[str] | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in items or []:
        if ":" not in item:
            raise SystemExit(f"invalid header (expected Key: Value): {item}")
        k, v = item.split(":", 1)
        headers[k.strip()] = v.strip()
    return headers


def _coalesce(arg, cfg: dict, key: str, default):
    if arg is not None:
        return arg
    if key in cfg and cfg[key] is not None:
        return cfg[key]
    return default


def _read_url_list(path: str | None) -> list[str]:
    if not path:
        return []
    urls: list[str] = []
    for line in Path(path).read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        # allow "url\toutput" simple form? keep simple: first token is URL if it looks like URL
        urls.append(s.split()[0])
    return urls


def _client_config(args, cfg: dict):
    from .http import ClientConfig

    headers = _parse_headers(_coalesce(args.header, cfg, "headers", []))
    cfg_headers = cfg.get("default_headers")
    if isinstance(cfg_headers, dict):
        merged = dict(cfg_headers)
        merged.update(headers)
        headers = merged
    return ClientConfig(
        timeout=float(_coalesce(args.timeout, cfg, "timeout", 30.0)),
        retries=int(_coalesce(args.retries, cfg, "retries", 3)),
        backoff=float(_coalesce(args.backoff, cfg, "backoff", 1.0)),
        max_redirects=int(_coalesce(args.max_redirects, cfg, "max_redirects", 10)),
        headers=headers,
        insecure=bool(_coalesce(args.insecure, cfg, "insecure", False)),
        ca_file=_coalesce(args.ca_file, cfg, "ca_file", None),
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(getattr(args, "config", None))
    cfg = merged_get_options(config)

    def _is_ftp_url(url: str) -> bool:
        return url.lower().startswith("ftp://") or url.lower().startswith("ftps://")

    def _ftp_config(args, cfg: dict) -> "FTPConfig":
        from .ftp import FTPConfig
        ftp_cfg = cfg.get("ftp", {})
        passive = True
        if args.ftp_active:
            passive = False
        elif args.ftp_passive:
            passive = True
        else:
            passive = _coalesce(None, ftp_cfg, "passive", True)
        ftps = bool(_coalesce(args.ftps, ftp_cfg, "ftps", False))
        tls_verify = True
        if args.ftp_no_tls_verify:
            tls_verify = False
        elif args.ftp_tls_verify:
            tls_verify = True
        else:
            tls_verify = _coalesce(None, ftp_cfg, "tls_verify", True)
        return FTPConfig(
            host="",  # filled per URL
            port=21,
            user="anonymous",
            passwd="anonymous@",
            passive=passive,
            ftps=ftps,
            tls_verify=tls_verify,
            ca_file=_coalesce(args.ca_file, ftp_cfg, "ca_file", None),
            timeout=float(_coalesce(args.timeout, cfg, "timeout", 30.0)),
        )

    if args.command == "head":
        if _is_ftp_url(args.url):
            from .ftp import probe_ftp
            import urllib.parse
            p = urllib.parse.urlparse(args.url)
            ftps = p.scheme == "ftps"
            info = probe_ftp(
                args.url,
                timeout=float(_coalesce(args.timeout, cfg, "timeout", 30.0)),
                ftps=ftps,
                tls_verify=not _coalesce(args.insecure, cfg, "insecure", False),
            )
        else:
            from .http import probe
            info = probe(args.url, _client_config(args, cfg))
        for k, v in info.items():
            print(f"{k}: {v}")
        return 0

    if args.command == "get":
        from .downloader import IntegrityError, default_progress_hook, download_with_mirrors
        from .ftp import FTPConfig, FTPSegmentManager, download_ftp, probe_ftp
        from .ratelimit import SpeedLimiter

        urls = list(getattr(args, "urls", []) or [])
        urls.extend(_read_url_list(getattr(args, "input_file", None)))
        cfg_urls = cfg.get("urls")
        if not urls and isinstance(cfg_urls, list):
            urls = [str(u) for u in cfg_urls]
        if not urls:
            parser.error("get requires URL(s) or -i/--input-file")
        if args.output and len(urls) != 1:
            parser.error("-o/--output can only be used with a single URL")
        if any([args.sha256, args.sha1, args.md5]) and len(urls) != 1:
            parser.error("--sha256/--sha1/--md5 can only be used with a single URL")

        client_cfg = _client_config(args, cfg)
        hook = None if bool(_coalesce(args.quiet, cfg, "quiet", False)) else default_progress_hook
        conns = int(_coalesce(args.connections, cfg, "connections", 1) or 1)
        conns = max(1, conns)
        limit = parse_speed(_coalesce(args.speed_limit, cfg, "limit_rate", None))
        resume = not bool(_coalesce(args.no_resume, cfg, "no_resume", False))
        cont = bool(_coalesce(getattr(args, "continue_on_error", None), cfg, "continue_on_error", False))
        out_dir = _coalesce(args.output_dir, cfg, "dir", None)
        template = _coalesce(getattr(args, "name_template", None), cfg, "name_template", None)

        # Mirror config from file
        mirror_cfg = cfg.get("mirrors", {})
        mirror_strategy = _coalesce(getattr(args, "mirror_strategy", None), mirror_cfg, "strategy", "fallback")
        mirror_probe_timeout = float(_coalesce(getattr(args, "mirror_probe_timeout", None), mirror_cfg, "probe_timeout", 5.0))
        mirror_urls = _coalesce(getattr(args, "mirror", None), mirror_cfg, "urls", [])
        if isinstance(mirror_urls, str):
            mirror_urls = [mirror_urls]
        mirror_file = _coalesce(getattr(args, "mirror_file", None), mirror_cfg, "mirror_file", None)

        # FTP config
        ftp_base_cfg = _ftp_config(args, cfg)

        failures = 0
        for i, url in enumerate(urls, 1):
            try:
                if _is_ftp_url(url):
                    # FTP/FTPS download
                    import urllib.parse
                    p = urllib.parse.urlparse(url)
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
                        passive=ftp_base_cfg.passive,
                        ftps=ftp_base_cfg.ftps or (p.scheme == "ftps"),
                        tls_verify=ftp_base_cfg.tls_verify,
                        ca_file=ftp_base_cfg.ca_file,
                        timeout=ftp_base_cfg.timeout,
                    )

                    from .naming import expand_output
                    from .downloader import IntegrityError, verify_integrity
                    from .ratelimit import SpeedLimiter

                    # Probe first to get filename
                    info = probe_ftp(url, timeout=ftp_cfg.timeout, ftps=ftp_cfg.ftps, tls_verify=ftp_cfg.tls_verify)
                    named = expand_output(template, url=url, info=info, index=i, directory=out_dir)
                    dest = Path(args.output or (str(named) if named else None) or info.get("filename") or "download").expanduser()
                    if dest.exists() and dest.is_dir():
                        dest = dest / (info.get("filename") or "download")

                    limiter = SpeedLimiter(limit)
                    if conns > 1:
                        # Use parallel segment manager
                        with FTPSegmentManager(ftp_cfg, progress=hook, speed_limit_bps=limit) as mgr:
                            mgr.download(
                                url, args.output if len(urls) == 1 else None,
                                connections=conns, resume=resume,
                                sha256=args.sha256 if len(urls) == 1 else None,
                                sha1=args.sha1 if len(urls) == 1 else None,
                                md5=args.md5 if len(urls) == 1 else None,
                                output_dir=out_dir, output_template=template, index=i,
                            )
                    else:
                        # Single stream
                        download_ftp(
                            url, dest,
                            resume=resume,
                            timeout=ftp_cfg.timeout,
                            chunk_size=256 * 1024,
                            progress=hook,
                            limiter=limiter,
                            ftps=ftp_cfg.ftps,
                            tls_verify=ftp_cfg.tls_verify,
                            passive=ftp_cfg.passive,
                        )
                        # Verify integrity
                        verify_integrity(dest, size=info.get("size"), sha256=args.sha256, sha1=args.sha1, md5=args.md5)
                    if not args.quiet:
                        print(dest)
                else:
                    # HTTP/HTTPS download with mirrors
                    mirror_set = build_mirror_set(
                        primary_url=url,
                        mirror_args=mirror_urls if len(urls) == 1 else [],
                        mirror_file=mirror_file if len(urls) == 1 else None,
                        strategy=mirror_strategy,
                        probe_timeout=mirror_probe_timeout,
                    )

                    chosen_url, path = download_with_mirrors(
                        mirror_set,
                        config=client_cfg,
                        progress=hook,
                        speed_limit_bps=limit,
                        connections=conns,
                        resume=resume,
                        sha256=args.sha256 if len(urls) == 1 else None,
                        sha1=args.sha1 if len(urls) == 1 else None,
                        md5=args.md5 if len(urls) == 1 else None,
                        output=args.output if len(urls) == 1 else None,
                        output_dir=out_dir,
                        output_template=template,
                        index=i,
                    )
                    if not args.quiet:
                        if chosen_url != url:
                            print(f"  (via mirror: {chosen_url})", file=sys.stderr)
                        print(path)
            except (IntegrityError, OSError, urllib.error.URLError, ValueError, RuntimeError) as e:
                failures += 1
                print(f"error: {url}: {e}", file=sys.stderr)
                if not cont and len(urls) > 1:
                    return 1
        return 1 if failures else 0

    if args.command == "web":
        from .web.main import run_web_server, WebConfig
        import asyncio

        config = load_config(getattr(args, "config", None))
        cfg = merged_get_options(config)
        web_cfg = cfg.get("web", {})

        host = args.host or web_cfg.get("host", "127.0.0.1")
        port = args.port or web_cfg.get("port", 8080)
        static_dir = args.static_dir or web_cfg.get("static_dir")
        max_concurrent = args.max_concurrent or web_cfg.get("max_concurrent", 3)

        wc = WebConfig(
            host=host,
            port=port,
            static_dir=Path(static_dir) if static_dir else None,
            max_concurrent=max_concurrent,
        )

        try:
            asyncio.run(run_web_server(wc))
            return 0
        except KeyboardInterrupt:
            return 0
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    parser.print_help()
    return 0
