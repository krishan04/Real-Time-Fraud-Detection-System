from fastapi import APIRouter, Request

from .. import db

router = APIRouter(tags=["summary"])


@router.get("/summary")
async def get_summary(request: Request):
    async with request.app.state.pool.acquire() as conn:
        return await db.summary(conn)