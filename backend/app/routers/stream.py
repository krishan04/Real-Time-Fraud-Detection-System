import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .. import config

router = APIRouter(tags=["stream"])


@router.get("/stream")
async def stream(request: Request):
    hub = request.app.state.hub
    queue = hub.subscribe()

    async def events():
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                try:
                    payload = await asyncio.wait_for(
                        queue.get(), timeout=config.SSE_KEEPALIVE_S
                    )
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                yield f"event: alert\ndata: {json.dumps(payload)}\n\n"
        finally:
            hub.unsubscribe(queue)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )