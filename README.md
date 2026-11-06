# mydl

[![PyPI Version](https://img.shields.io/pypi/v/mydl-cli?label=)](https://pypi.org/project/mydl-cli/)
[![Python Versions](https://img.shields.io/pypi/pyversions/mydl-cli)](https://pypi.org/project/mydl-cli/)
[![License](https://img.shields.io/pypi/l/mydl-cli)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/sc8134/MY-DL/pypi-publish.yml?branch=main&label=build)](https://github.com/sc8134/MY-DL/actions)
[![Downloads](https://img.shields.io/pypi/dm/mydl-cli)](https://pypi.org/project/mydl-cli/)

> **A zero-dependency, stdlib-only download manager** with mirror support, FTP/FTPS, parallel downloads, resume capability, integrity verification, and a built-in Web UI dashboard.

---

## Why mydl?

| Feature | Benefit |
|---------|---------|
| **Zero dependencies** | Runs on Python 3.11+ standard library only — no supply-chain risk |
| **Mirrors** | Fallback, parallel race, or fastest-probe strategies for reliability |
| **FTP/FTPS** | Full explicit TLS support, parallel range downloads, MLSD listings |
| **Parallel HTTP** | Multi-connection with dynamic segment splitting, adaptive workers |
| **Crash-safe resume** | `.part` files + `.mydl.json` state survive interruption |
| **Integrity** | SHA-256, SHA-1, MD5 verification on download |
| **Rate limiting** | Per-download throttling (`--limit-rate 2M`) |
| **Batch/Queue** | Multiple URLs, input files, flexible naming templates |
| **Web UI** | Real-time dashboard at `mydl web` with WebSocket updates |
| **Tiny** | ~50 KB wheel, installs in seconds |

---

## Installation

```bash
pip install mydl-cli
```

Requires **Python 3.11+**. No system packages, no Rust, no C extensions.

---

## Quick Start

```bash
# Download with 4 parallel connections + SHA-256 verification
mydl get https://example.com/file.iso -n 4 --sha256 abc123... -o file.iso

# Batch from URL list with naming template
mydl get -i urls.txt -d downloads -O "{index}-{filename}" -n 4

# Use mirrors — auto-probe fastest
mydl get https://primary/file.iso --mirror https://mirror1/file.iso --mirror-strategy fastest

# FTP with parallel connections
mydl get ftp://ftp.gnu.org/README -n 2

# Launch Web UI dashboard
mydl web --port 8080
# Open http://localhost:8080
```

---

## Commands

### `mydl get` — Download Files

```bash
mydl get [URLs...] [options]
```

| Category | Options |
|----------|---------|
| **Input** | `-i, --input-file FILE` — URLs (one/line, `#` comments)<br>`--mirror URL` — Add mirror (repeatable)<br>`--mirror-file FILE` — Mirror list<br>`--mirror-strategy` — `fallback` \| `parallel` \| `fastest`<br>`--mirror-probe-timeout SEC` |
| **Output** | `-o, --output PATH` — Output file (single URL)<br>`-d, --dir DIR` — Output directory<br>`-O, --name-template TMPL` — `{filename}`, `{name}`, `{ext}`, `{index}`, `{host}` |
| **Download** | `-n, --connections N` — Parallel connections (default: 1)<br>`--limit-rate RATE` — Speed limit (e.g. `512K`, `2M`, `10M`)<br>`--no-resume` — Ignore existing `.part`/`.mydl.json`<br>`-q, --quiet` — Suppress progress |
| **Integrity** | `--sha256 HASH` — Verify SHA-256<br>`--sha1 HASH` — Verify SHA-1<br>`--md5 HASH` — Verify MD5 |
| **FTP/FTPS** | `--ftp-passive` / `--ftp-active`<br>`--ftps` — Use FTPS (explicit TLS)<br>`--ftp-tls-verify` / `--ftp-no-tls-verify` |
| **HTTP** | `--timeout SEC`, `--retries N`, `--backoff SEC`, `--max-redirects N`<br>`-H, --header K:V` — Custom headers<br>`-k, --insecure` — Disable TLS verify<br>`--ca-file PATH` — Custom CA bundle |
| **Other** | `--continue-on-error` — Continue queue on failure |

### `mydl head` — Probe URL Metadata

```bash
mydl head https://example.com/file.iso
```

Returns: `url`, `status`, `size`, `accept_ranges`, `content_type`, `etag`, `last_modified`, `filename`

### `mydl verify` — Check Local File Integrity

```bash
mydl verify file.iso --sha256 abc123... --size 123456789
```

### `mydl web` — Launch Web UI

```bash
mydl web --port 8080 [--host 0.0.0.0] [--max-concurrent 3]
```

Features:
- Real-time progress via WebSocket
- Add URLs with mirrors, connections, speed limits
- Pause/Resume/Cancel/Remove tasks
- Concurrent download limiting

---

## Configuration

Create `.mydl.toml` in project or home directory (auto-discovered):

```toml
[mirrors]
strategy = "fastest"           # fallback | parallel | fastest
probe_timeout = 5.0
urls = ["https://mirror1/", "https://mirror2/"]

[ftp]
passive = true
ftps = false
tls_verify = true

[web]
host = "127.0.0.1"
port = 8080
max_concurrent = 3
```

Or specify explicitly: `mydl --config /path/to/config.toml get ...`

---

## Examples

### Large File with Mirrors & Verification
```bash
mydl get https://releases.ubuntu.com/22.04/ubuntu-22.04.3.iso \
  --mirror https://mirror1.ubuntu.com/22.04.3.iso \
  --mirror https://mirror2.ubuntu.com/22.04.3.iso \
  --mirror-strategy fastest \
  -n 8 \
  --sha256 a1b2c3d4... \
  -o ubuntu-22.04.3.iso
```

### Batch Game Updates
```bash
mydl get -i game_patches.txt -d patches -O "{index}-{filename}" -n 4 --limit-rate 10M
```

### FTP Directory Download (Web UI)
```bash
mydl web --port 8080
# Add ftp://ftp.example.com/game_update.zip in dashboard with -n 4
```

---

## Architecture

```
mydl/
├── cli.py           # Argument parsing, command dispatch
├── config.py        # TOML/JSON config loading
├── http.py          # HTTP/1.1 client (urllib + cookies + redirects)
├── ftp.py           # FTP/FTPS client (ftplib + ssl)
├── mirrors.py       # MirrorSet + strategies (fallback/parallel/fastest)
├── downloader.py    # SingleStream + SegmentManager (parallel ranges)
├── naming.py        # Output path templating
├── ratelimit.py     # Token-bucket speed limiter
└── web/             # Async HTTP server + WebSocket + Dashboard
    ├── server.py    # HTTP/1.1 + RFC 6455 WebSocket
    ├── queue.py     # DownloadQueue (concurrent task management)
    ├── api.py       # REST API + WebSocket handler
    ├── static.py    # Embedded SPA (vanilla JS)
    └── main.py      # Entry point
```

**Concurrency model**: ThreadPoolExecutor for blocking I/O (downloads), asyncio for Web UI. Per-worker file handles on Windows (no `os.pwrite`).

---

## Development

```bash
git clone https://github.com/sc8134/MY-DL
cd MY-DL
pip install -e .[dev]    # Includes pytest, ruff, mypy
mydl --version
```

Run tests:
```bash
pytest -v
```

Type-check:
```bash
mypy src/mydl
```

Lint:
```bash
ruff check src/mydl
```

---

## License

MIT — see [LICENSE](LICENSE)

---

## Acknowledgments

Built entirely on Python's standard library. Inspired by `aria2`, `wget`, `curl`, and `yt-dlp` — but with zero dependencies.