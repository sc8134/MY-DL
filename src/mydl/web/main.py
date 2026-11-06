"""Main web UI entry point."""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .server import HTTPServer, HTTPRouter
from .queue import DownloadQueue
from .api import create_api_router
from .static import get_dashboard_html


@dataclass
class WebConfig:
    host: str = "127.0.0.1"
    port: int = 8080
    static_dir: Optional[Path] = None
    max_concurrent: int = 3


async def run_web_server(config: WebConfig) -> None:
    """Run the web UI server."""
    queue = DownloadQueue(max_concurrent=config.max_concurrent)

    # Create router with API
    router = create_api_router(queue)

    # Serve dashboard
    async def serve_dashboard(request):
        from .server import HTTPResponse
        return HTTPResponse(200, body=get_dashboard_html().encode(), content_type="text/html")

    router.add_route("GET", "/", serve_dashboard)

    # Serve static files if directory provided
    if config.static_dir:
        router.set_static(config.static_dir)

    server = HTTPServer(config.host, config.port, router)

    try:
        await server.start()
        # Keep running
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        print("\nShutting down...", file=sys.stderr)
    finally:
        await queue.shutdown()
        await server.stop()


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(prog="mydl web", description="Start web UI")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address")
    parser.add_argument("--port", type=int, default=8080, help="Port")
    parser.add_argument("--static-dir", help="Static files directory")
    parser.add_argument("--max-concurrent", type=int, default=3, help="Max concurrent downloads")
    args = parser.parse_args(sys.argv[2:])

    config = WebConfig(
        host=args.host,
        port=args.port,
        static_dir=Path(args.static_dir) if args.static_dir else None,
        max_concurrent=args.max_concurrent,
    )

    try:
        asyncio.run(run_web_server(config))
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())