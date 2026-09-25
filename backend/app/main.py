import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import asyncpg
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from . import config, db
from .kafka import AlertConsumer, AlertHub
from .routers import alerts, metrics, stream, summary, transactions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fraud.backend")


async def _wait_for_db(retries: int = 30, delay: float = 2.0) -> asyncpg.Pool:
    last = None
    for _ in range(retries):
        try:
            return await db.create_pool()
        except (OSError, asyncpg.PostgresError) as exc:
            last = exc
            await asyncio.sleep(delay)
    raise RuntimeError("could not connect to postgres") from last


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await _wait_for_db()
    app.state.pool = pool
    hub = AlertHub()
    app.state.hub = hub
    consumer = AlertConsumer(hub)
    await consumer.start()
    app.state.consumer = consumer
    logger.info("backend ready")
    try:
        yield
    finally:
        await consumer.stop()
        await pool.close()


app = FastAPI(title="Fraud Detection API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

for router in (summary.router, transactions.router, alerts.router, metrics.router, stream.router):
    app.include_router(router, prefix="/api")


@app.get("/api/health")
async def health(request: Request):
    hub: AlertHub = request.app.state.hub
    db_ok = True
    try:
        async with request.app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception:
        db_ok = False
    return {
        "status": "ok" if db_ok else "degraded",
        "db": db_ok,
        "stream": hub.stats,
        "time": datetime.now(timezone.utc).isoformat(),
    }