"""Phase 1 HTTP client: headers, redirects, cookies, retries, timeouts, HEAD probing.

Stdlib only. Uses urllib for portability; wraps behavior explicitly for control.
"""

from __future__ import annotations

import http.cookiejar
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

DEFAULT_UA = "mydl/0.1 (+https://example.invalid)"
RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


@dataclass
class ClientConfig:
    timeout: float = 30.0
    retries: int = 3
    backoff: float = 1.0
    max_redirects: int = 10
    headers: dict[str, str] = field(default_factory=dict)
    insecure: bool = False
    ca_file: str | None = None


class _RedirectLimiter(urllib.request.HTTPRedirectHandler):
    def __init__(self, max_redirects: int) -> None:
        self.max_redirects = max_redirects

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        # urllib tracks redirects internally; enforce a hard cap.
        redirects = getattr(req, "redirect_dict", {})
        if len(redirects) >= self.max_redirects:
            raise urllib.error.HTTPError(
                newurl, code, f"too many redirects (>{self.max_redirects})", headers, fp
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class HTTPClient:
    def __init__(self, config: ClientConfig | None = None) -> None:
        self.config = config or ClientConfig()
        self.cookies = http.cookiejar.CookieJar()
        handlers: list[Any] = [
            urllib.request.HTTPCookieProcessor(self.cookies),
            _RedirectLimiter(self.config.max_redirects),
        ]
        https = self._https_handler()
        if https is not None:
            handlers.append(https)
        self._opener = urllib.request.build_opener(*handlers)

    def _https_handler(self) -> urllib.request.HTTPSHandler | None:
        if self.config.ca_file:
            ctx = ssl.create_default_context(cafile=self.config.ca_file)
            return urllib.request.HTTPSHandler(context=ctx)
        if self.config.insecure:
            ctx = ssl._create_unverified_context()
            return urllib.request.HTTPSHandler(context=ctx)
        # Try default; environments with broken CA stores can still use --insecure/--ca-file.
        try:
            ssl.create_default_context()
        except Exception:
            return None
        return None

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        h = {"User-Agent": DEFAULT_UA, **self.config.headers}
        if extra:
            h.update(extra)
        return h

    def open(self, url: str, method: str = "GET", headers: dict[str, str] | None = None):
        req = urllib.request.Request(url, method=method, headers=self._headers(headers))
        last: Exception | None = None
        for attempt in range(self.config.retries + 1):
            try:
                return self._opener.open(req, timeout=self.config.timeout)
            except urllib.error.HTTPError as e:
                if e.code in RETRY_STATUS and attempt < self.config.retries:
                    last = e
                    time.sleep(self.config.backoff * (2**attempt))
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last = e
                if attempt < self.config.retries:
                    time.sleep(self.config.backoff * (2**attempt))
                    continue
                raise
        if last:
            raise last
        raise RuntimeError("unreachable")

    def head(self, url: str) -> dict[str, Any]:
        try:
            with self.open(url, method="HEAD") as resp:
                info = self._info_from_response(url, resp)
        except urllib.error.HTTPError as e:
            if e.code in {405, 501}:
                with self.open(url, method="GET", headers={"Range": "bytes=0-0"}) as resp:
                    info = self._info_from_response(url, resp)
                # 206 from range probe implies byte ranges are supported.
                if getattr(resp, "status", None) == 206:
                    info["accept_ranges"] = True
                return info
            raise
        # Some servers omit Accept-Ranges but honor Range requests; leave as reported.
        return info

    @staticmethod
    def _info_from_response(url: str, resp: Any) -> dict[str, Any]:
        headers = {k.lower(): v for k, v in resp.headers.items()}
        length = headers.get("content-length")
        cr = headers.get("content-range")
        total = None
        if cr and "/" in cr:
            tail = cr.rsplit("/", 1)[1].strip()
            if tail.isdigit():
                total = int(tail)
        elif length and length.isdigit():
            total = int(length)
        accept = headers.get("accept-ranges", "").lower()
        return {
            "url": resp.geturl() or url,
            "status": getattr(resp, "status", None),
            "size": total,
            "accept_ranges": "bytes" in accept,
            "content_type": headers.get("content-type"),
            "etag": headers.get("etag"),
            "last_modified": headers.get("last-modified"),
            "filename": _filename_from_headers(headers, resp.geturl() or url),
        }


def probe(url: str, config: ClientConfig | None = None) -> dict[str, Any]:
    return HTTPClient(config).head(url)


def _filename_from_headers(headers: dict[str, str], url: str) -> str | None:
    cd = headers.get("content-disposition", "")
    if "filename*=" in cd:
        part = cd.split("filename*=", 1)[1].split(";", 1)[0].strip()
        if "''" in part:
            part = part.split("''", 1)[1]
        return urllib.parse.unquote(part.strip('"'))
    if "filename=" in cd:
        part = cd.split("filename=", 1)[1].split(";", 1)[0].strip().strip('"')
        return part or None
    path = urllib.parse.urlparse(url).path
    name = path.rsplit("/", 1)[-1]
    return urllib.parse.unquote(name) or None
