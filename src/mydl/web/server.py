"""Async HTTP/1.1 server with WebSocket support (stdlib only)."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import html
import json
import mimetypes
import os
import re
import sys
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

try:
    from http.cookies import SimpleCookie
except ImportError:
    SimpleCookie = None


@dataclass
class HTTPRequest:
    method: str
    path: str
    version: str
    headers: Dict[str, str]
    body: bytes
    query: Dict[str, List[str]]
    cookies: Dict[str, str]

    @property
    def full_path(self) -> str:
        return self.path


@dataclass
class HTTPResponse:
    status: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    body: bytes = b""
    content_type: str = "text/plain"

    def to_bytes(self) -> bytes:
        headers = dict(self.headers)
        headers.setdefault("Content-Type", self.content_type)
        headers.setdefault("Content-Length", str(len(self.body)))
        header_lines = [f"{k}: {v}" for k, v in headers.items()]
        header_block = "\r\n".join(header_lines)
        return f"HTTP/1.1 {self.status} {self._status_text()}\r\n{header_block}\r\n\r\n".encode() + self.body

    def _status_text(self) -> str:
        texts = {
            200: "OK", 201: "Created", 204: "No Content",
            301: "Moved Permanently", 302: "Found", 304: "Not Modified",
            400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
            404: "Not Found", 405: "Method Not Allowed", 409: "Conflict",
            500: "Internal Server Error", 501: "Not Implemented", 503: "Service Unavailable",
        }
        return texts.get(self.status, "OK")


class WebSocketProtocolError(Exception):
    pass


class WebSocketFrame:
    OPCODE_CONTINUATION = 0x0
    OPCODE_TEXT = 0x1
    OPCODE_BINARY = 0x2
    OPCODE_CLOSE = 0x8
    OPCODE_PING = 0x9
    OPCODE_PONG = 0xA

    def __init__(
        self,
        opcode: int,
        payload: bytes = b"",
        fin: bool = True,
        masked: bool = False,
        mask_key: bytes = b"",
    ):
        self.opcode = opcode
        self.payload = payload
        self.fin = fin
        self.masked = masked
        self.mask_key = mask_key

    @classmethod
    def parse(cls, data: bytes) -> Tuple["WebSocketFrame", int]:
        """Parse a WebSocket frame from data. Returns (frame, bytes_consumed)."""
        if len(data) < 2:
            raise WebSocketProtocolError("Incomplete frame header")

        first = data[0]
        second = data[1]
        fin = bool(first & 0x80)
        rsv = (first >> 4) & 0x7
        opcode = first & 0xF
        masked = bool(second & 0x80)
        payload_len = second & 0x7F

        if rsv != 0:
            raise WebSocketProtocolError(f"Reserved bits set: {rsv}")

        offset = 2
        if payload_len == 126:
            if len(data) < offset + 2:
                raise WebSocketProtocolError("Incomplete extended length (16-bit)")
            payload_len = int.from_bytes(data[offset:offset+2], "big")
            offset += 2
        elif payload_len == 127:
            if len(data) < offset + 8:
                raise WebSocketProtocolError("Incomplete extended length (64-bit)")
            payload_len = int.from_bytes(data[offset:offset+8], "big")
            offset += 8

        if masked:
            if len(data) < offset + 4:
                raise WebSocketProtocolError("Incomplete mask key")
            mask_key = data[offset:offset+4]
            offset += 4
        else:
            mask_key = b""

        if len(data) < offset + payload_len:
            raise WebSocketProtocolError("Incomplete frame payload")

        payload = data[offset:offset+payload_len]
        if masked:
            payload = cls._apply_mask(payload, mask_key)

        return cls(opcode, payload, fin, masked, mask_key), offset + payload_len

    @staticmethod
    def _apply_mask(data: bytes, mask_key: bytes) -> bytes:
        return bytes(b ^ mask_key[i % 4] for i, b in enumerate(data))

    def to_bytes(self, mask: bool = False) -> bytes:
        """Serialize frame to bytes. Client->server frames must be masked."""
        first = 0x80 if self.fin else 0
        first |= self.opcode

        payload = self.payload
        mask_key = b""
        if mask:
            import secrets
            mask_key = secrets.token_bytes(4)
            payload = self._apply_mask(payload, mask_key)

        length = len(payload)
        if length < 126:
            second = length | (0x80 if mask else 0)
            header = bytes([first, second])
        elif length < 65536:
            second = 126 | (0x80 if mask else 0)
            header = bytes([first, second]) + length.to_bytes(2, "big")
        else:
            second = 127 | (0x80 if mask else 0)
            header = bytes([first, second]) + length.to_bytes(8, "big")

        if mask:
            header += mask_key
        return header + payload

    @classmethod
    def text(cls, text: str, fin: bool = True) -> "WebSocketFrame":
        return cls(cls.OPCODE_TEXT, text.encode("utf-8"), fin=fin)

    @classmethod
    def binary(cls, data: bytes, fin: bool = True) -> "WebSocketFrame":
        return cls(cls.OPCODE_BINARY, data, fin=fin)

    @classmethod
    def close(cls, code: int = 1000, reason: str = "") -> "WebSocketFrame":
        payload = code.to_bytes(2, "big") + reason.encode("utf-8")
        return cls(cls.OPCODE_CLOSE, payload, fin=True)

    @classmethod
    def ping(cls, data: bytes = b"") -> "WebSocketFrame":
        return cls(cls.OPCODE_PING, data, fin=True)

    @classmethod
    def pong(cls, data: bytes = b"") -> "WebSocketFrame":
        return cls(cls.OPCODE_PONG, data, fin=True)


class WebSocketConnection:
    """WebSocket connection handler."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.closed = False
        self._send_lock = asyncio.Lock()

    async def send_frame(self, frame: WebSocketFrame) -> None:
        async with self._send_lock:
            if self.closed:
                return
            self.writer.write(frame.to_bytes(mask=False))
            await self.writer.drain()

    async def send_text(self, text: str) -> None:
        await self.send_frame(WebSocketFrame.text(text))

    async def send_json(self, obj: Any) -> None:
        await self.send_text(json.dumps(obj))

    async def send_binary(self, data: bytes) -> None:
        await self.send_frame(WebSocketFrame.binary(data))

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if not self.closed:
            self.closed = True
            try:
                await self.send_frame(WebSocketFrame.close(code, reason))
            except Exception:
                pass

    async def ping(self) -> None:
        await self.send_frame(WebSocketFrame.ping())

    async def recv_frame(self) -> Optional[WebSocketFrame]:
        """Receive a single frame, handling fragmentation."""
        buffer = bytearray()
        while not self.closed:
            try:
                chunk = await self.reader.read(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                while len(buffer) >= 2:
                    try:
                        frame, consumed = WebSocketFrame.parse(bytes(buffer))
                        del buffer[:consumed]
                        return frame
                    except WebSocketProtocolError as e:
                        if "Incomplete" in str(e):
                            break
                        raise
            except Exception:
                break
        return None

    async def recv_message(self) -> Optional[Union[str, bytes]]:
        """Receive a complete message (handles fragmentation)."""
        fragments: List[bytes] = []
        opcode = None
        while not self.closed:
            frame = await self.recv_frame()
            if frame is None:
                return None
            if frame.opcode in (WebSocketFrame.OPCODE_TEXT, WebSocketFrame.OPCODE_BINARY):
                if opcode is None:
                    opcode = frame.opcode
                elif frame.opcode != WebSocketFrame.OPCODE_CONTINUATION:
                    raise WebSocketProtocolError("Invalid fragmented message")
                fragments.append(frame.payload)
                if frame.fin:
                    break
            elif frame.opcode == WebSocketFrame.OPCODE_CONTINUATION:
                if opcode is None:
                    raise WebSocketProtocolError("Continuation without start")
                fragments.append(frame.payload)
                if frame.fin:
                    break
            elif frame.opcode == WebSocketFrame.OPCODE_CLOSE:
                await self.close(1000, "")
                return None
            elif frame.opcode == WebSocketFrame.OPCODE_PING:
                await self.send_frame(WebSocketFrame.pong(frame.payload))
            elif frame.opcode == WebSocketFrame.OPCODE_PONG:
                pass  # Ignore
        if not fragments:
            return None
        data = b"".join(fragments)
        if opcode == WebSocketFrame.OPCODE_TEXT:
            return data.decode("utf-8")
        return data


async def websocket_handshake(request: HTTPRequest) -> Tuple[bytes, Dict[str, str]]:
    """Perform WebSocket handshake. Returns (response_headers_bytes, accepted_headers)."""
    key = request.headers.get("sec-websocket-key", "")
    if not key:
        raise ValueError("Missing Sec-WebSocket-Key")
    version = request.headers.get("sec-websocket-version", "")
    if version != "13":
        raise ValueError(f"Unsupported WebSocket version: {version}")

    # Compute accept key
    GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    accept_key = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()

    headers = {
        "Upgrade": "websocket",
        "Connection": "Upgrade",
        "Sec-WebSocket-Accept": accept_key,
    }
    return (b"HTTP/1.1 101 Switching Protocols\r\n" +
            "\r\n".join(f"{k}: {v}" for k, v in headers.items()).encode() + b"\r\n\r\n", headers)


def parse_http_request(data: bytes) -> Tuple[HTTPRequest, int]:
    """Parse HTTP request from bytes. Returns (request, bytes_consumed)."""
    # Find end of headers
    header_end = data.find(b"\r\n\r\n")
    if header_end == -1:
        raise ValueError("Incomplete HTTP request")

    header_block = data[:header_end].decode("iso-8859-1")
    lines = header_block.split("\r\n")
    if not lines:
        raise ValueError("Empty request")

    # Parse request line
    request_line = lines[0].strip()
    parts = request_line.split(" ", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid request line: {request_line}")
    method, full_path, version = parts

    # Parse headers
    headers = {}
    for line in lines[1:]:
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        headers[k.strip().lower()] = v.strip()

    # Parse query string
    parsed = urllib.parse.urlparse(full_path)
    path = parsed.path
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    # Parse cookies
    cookies = {}
    cookie_header = headers.get("cookie", "")
    if cookie_header and SimpleCookie:
        cookie_jar = SimpleCookie()
        cookie_jar.load(cookie_header)
        for k, morsel in cookie_jar.items():
            cookies[k] = morsel.value

    # Read body
    content_length = int(headers.get("content-length", "0"))
    body_start = header_end + 4
    body = data[body_start:body_start + content_length]
    if len(body) < content_length:
        raise ValueError("Incomplete request body")

    return HTTPRequest(
        method=method,
        path=path,
        version=version,
        headers=headers,
        body=body,
        query=query,
        cookies=cookies,
    ), body_start + content_length


class HTTPRouter:
    def __init__(self):
        self.routes: List[Tuple[str, str, Callable]] = []  # (method, pattern, handler)
        self.static_dir: Optional[Path] = None

    def add_route(self, method: str, pattern: str, handler: Callable):
        self.routes.append((method.upper(), pattern, handler))

    def set_static(self, directory: Path):
        self.static_dir = directory.resolve()

    async def handle(self, request: HTTPRequest) -> HTTPResponse:
        # Check static files first
        if self.static_dir and request.method == "GET":
            static_response = await self._serve_static(request)
            if static_response:
                return static_response

        # Match routes
        for method, pattern, handler in self.routes:
            if method != request.method and method != "*":
                continue
            match = self._match_route(pattern, request.path)
            if match is not None:
                path_params, _ = match
                request.path_params = path_params  # type: ignore
                return await handler(request)

        return HTTPResponse(404, body=b"Not Found")

    def _match_route(self, pattern: str, path: str) -> Optional[Tuple[Dict[str, str], List[str]]]:
        """Match path against pattern. Returns (params, remaining_path_parts) or None."""
        # Convert pattern to regex
        regex_parts = []
        param_names = []
        for part in pattern.strip("/").split("/"):
            if part.startswith("{") and part.endswith("}"):
                name = part[1:-1]
                param_names.append(name)
                regex_parts.append(r"([^/]+)")
            else:
                regex_parts.append(re.escape(part))
        regex = "^/" + "/".join(regex_parts) + "/?$"
        match = re.match(regex, path)
        if not match:
            return None
        params = dict(zip(param_names, match.groups()))
        return params, []

    async def _serve_static(self, request: HTTPRequest) -> Optional[HTTPResponse]:
        if not self.static_dir:
            return None
        # Normalize path
        path = request.path.lstrip("/")
        if not path or path.endswith("/"):
            path += "index.html"
        file_path = (self.static_dir / path).resolve()
        try:
            file_path.relative_to(self.static_dir)
        except ValueError:
            return None  # Path traversal attempt

        if not file_path.exists() or not file_path.is_file():
            return None

        mime_type, _ = mimetypes.guess_type(str(file_path))
        if not mime_type:
            mime_type = "application/octet-stream"

        try:
            body = file_path.read_bytes()
        except Exception:
            return HTTPResponse(500, body=b"Internal Server Error")

        return HTTPResponse(200, body=body, content_type=mime_type)


class HTTPServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        router: Optional[HTTPRouter] = None,
    ):
        self.host = host
        self.port = port
        self.router = router or HTTPRouter()
        self.server: Optional[asyncio.Server] = None
        self._running = False

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self._running = True
        addrs = ", ".join(str(sock.getsockname()) for sock in self.server.sockets)
        print(f"Web server running on {addrs}", file=sys.stderr)

    async def stop(self) -> None:
        self._running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        buffer = bytearray()
        try:
            while self._running:
                data = await reader.read(8192)
                if not data:
                    break
                buffer.extend(data)
                while True:
                    try:
                        request, consumed = parse_http_request(bytes(buffer))
                        del buffer[:consumed]
                    except ValueError as e:
                        if "Incomplete" in str(e):
                            break
                        # Bad request
                        writer.write(HTTPResponse(400, body=f"Bad Request: {e}".encode()).to_bytes())
                        await writer.drain()
                        return

                    # Check for WebSocket upgrade
                    if (request.headers.get("upgrade", "").lower() == "websocket" and
                            request.headers.get("connection", "").lower() == "upgrade"):
                        await self._handle_websocket(request, reader, writer)
                        return

                    response = await self.router.handle(request)
                    writer.write(response.to_bytes())
                    await writer.drain()
                    if response.headers.get("connection", "").lower() == "close":
                        return
            # Handle remaining buffer (likely incomplete request)
        except Exception as e:
            print(f"Error handling client {peer}: {e}", file=sys.stderr)
        finally:
            writer.close()
            await writer.wait_closed()

    async def _handle_websocket(
        self,
        request: HTTPRequest,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            handshake_data, _ = await websocket_handshake(request)
            writer.write(handshake_data)
            await writer.drain()
        except Exception as e:
            writer.write(HTTPResponse(400, body=f"WebSocket handshake failed: {e}".encode()).to_bytes())
            await writer.drain()
            return

        ws = WebSocketConnection(reader, writer)
        # Find WebSocket handler
        handler = None
        for method, pattern, h in self.router.routes:
            if hasattr(h, "_ws_handler") and h._ws_handler:
                match = self.router._match_route(pattern, request.path)
                if match:
                    handler = h
                    break
        if handler:
            await handler(ws)
        else:
            await ws.close(1003, "No handler")