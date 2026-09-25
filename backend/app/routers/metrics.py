from fastapi import APIRouter, Query, Request

from .. import db

router = APIRouter(tags=["metrics"])


@router.get("/metrics")
async def get_metrics(
    request: Request,
    minutes: int = Query(60, ge=1, le=1440),
    table: str = Query("minute", pattern="^(minute|user_window)$"),
):
    async with request.app.state.pool.acquire() as conn:
        if table == "user_window":
            rows = await db.user_window_metrics(conn, minutes)
            return {"table": "user_window", "minutes": minutes, "rows": rows}
        rows = await db.minute_metrics(conn, minutes)
        return {"table": "minute", "minutes": minutes, "rows": rows}