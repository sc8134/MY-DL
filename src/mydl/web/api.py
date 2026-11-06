"""REST API endpoints for web UI."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional

from .server import HTTPRequest, HTTPResponse, HTTPRouter
from .queue import DownloadQueue, DownloadTask, TaskStatus


def create_api_router(queue: DownloadQueue) -> HTTPRouter:
    router = HTTPRouter()

    # Define handlers first
    async def handle_list_tasks(request: HTTPRequest) -> HTTPResponse:
        tasks = queue.list_tasks()
        return HTTPResponse(200, body=json.dumps([t.to_dict() for t in tasks]).encode(), content_type="application/json")

    async def handle_add_task(request: HTTPRequest) -> HTTPResponse:
        try:
            data = json.loads(request.body.decode() or "{}")
        except json.JSONDecodeError:
            return HTTPResponse(400, body=b"Invalid JSON")

        task = DownloadTask(
            id=str(uuid.uuid4())[:8],
            url=data.get("url", ""),
            mirrors=data.get("mirrors", []),
            mirror_strategy=data.get("mirror_strategy", "fallback"),
            mirror_probe_timeout=float(data.get("mirror_probe_timeout", 5.0)),
            connections=int(data.get("connections", 1)),
            speed_limit=data.get("speed_limit"),
            resume=data.get("resume", True),
            sha256=data.get("sha256"),
            sha1=data.get("sha1"),
            md5=data.get("md5"),
            output=data.get("output"),
            output_dir=data.get("output_dir"),
            output_template=data.get("output_template"),
            index=int(data.get("index", 1)),
            ftp_passive=data.get("ftp_passive", True),
            ftps=data.get("ftps", False),
            ftp_tls_verify=data.get("ftp_tls_verify", True),
        )
        if not task.url:
            return HTTPResponse(400, body=b"URL is required")

        queue.add_task(task)
        # Auto-start if capacity available
        await queue.start_task(task.id)
        return HTTPResponse(201, body=json.dumps(task.to_dict()).encode(), content_type="application/json")

    async def handle_get_task(request: HTTPRequest) -> HTTPResponse:
        task_id = getattr(request, "path_params", {}).get("id", "")
        task = queue.get_task(task_id)
        if not task:
            return HTTPResponse(404, body=b"Task not found")
        return HTTPResponse(200, body=json.dumps(task.to_dict()).encode(), content_type="application/json")

    async def handle_update_task(request: HTTPRequest) -> HTTPResponse:
        task_id = getattr(request, "path_params", {}).get("id", "")
        task = queue.get_task(task_id)
        if not task:
            return HTTPResponse(404, body=b"Task not found")

        try:
            data = json.loads(request.body.decode() or "{}")
        except json.JSONDecodeError:
            return HTTPResponse(400, body=b"Invalid JSON")

        action = data.get("action")
        if action == "pause":
            await queue.pause_task(task_id)
        elif action == "resume":
            await queue.resume_task(task_id)
        elif action == "cancel":
            await queue.cancel_task(task_id)
        else:
            return HTTPResponse(400, body=b"Invalid action")

        task = queue.get_task(task_id)
        return HTTPResponse(200, body=json.dumps(task.to_dict()).encode(), content_type="application/json")

    async def handle_delete_task(request: HTTPRequest) -> HTTPResponse:
        task_id = getattr(request, "path_params", {}).get("id", "")
        await queue.remove_task(task_id)
        return HTTPResponse(204)

    async def handle_websocket(ws) -> None:
        # Subscribe to updates
        sub_queue = queue.subscribe()
        try:
            # Send initial state
            tasks = queue.list_tasks()
            await ws.send_json({"type": "init", "tasks": [t.to_dict() for t in tasks]})

            # Forward updates
            while True:
                try:
                    update = await asyncio.wait_for(sub_queue.get(), timeout=30.0)
                    await ws.send_json({"type": "update", "task": update})
                except asyncio.TimeoutError:
                    await ws.ping()
        except Exception:
            pass
        finally:
            queue.unsubscribe(sub_queue)

    async def handle_health(request: HTTPRequest) -> HTTPResponse:
        return HTTPResponse(200, body=json.dumps({"status": "ok"}).encode(), content_type="application/json")

    # Add routes
    router.add_route("GET", "/api/queue", handle_list_tasks)
    router.add_route("POST", "/api/queue", handle_add_task)
    router.add_route("GET", "/api/queue/{id}", handle_get_task)
    router.add_route("PATCH", "/api/queue/{id}", handle_update_task)
    router.add_route("DELETE", "/api/queue/{id}", handle_delete_task)
    router.add_route("GET", "/api/ws", handle_websocket)
    router.add_route("GET", "/api/health", handle_health)

    return router