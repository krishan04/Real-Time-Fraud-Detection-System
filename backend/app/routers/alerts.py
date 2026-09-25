from fastapi import APIRouter, Query, Request

from .. import db

router = APIRouter(tags=["alerts"])


@router.get("/alerts")
async def list_alerts(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    severity: str | None = Query(None, pattern="^(HIGH|MEDIUM|LOW)$"),
    reason: str | None = Query(None, max_length=64),
):
    async with request.app.state.pool.acquire() as conn:
        return await db.alerts(conn, limit, offset, severity, reason)