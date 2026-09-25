from fastapi import APIRouter, Query, Request

from .. import db

router = APIRouter(tags=["transactions"])


@router.get("/transactions")
async def list_transactions(
    request: Request,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    flagged: bool | None = Query(None),
    user: str | None = Query(None, max_length=64),
):
    async with request.app.state.pool.acquire() as conn:
        return await db.transactions(conn, limit, offset, flagged, user)