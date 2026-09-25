import os

import httpx

BACKEND_URL = os.getenv("DASHBOARD_BACKEND_URL", "http://backend:8000")
_TIMEOUT = 10.0


def _get(path: str, **params) -> dict:
    with httpx.Client(base_url=BACKEND_URL, timeout=_TIMEOUT, follow_redirects=True) as client:
        response = client.get(path, params=params or None)
        response.raise_for_status()
        return response.json()


def fetch_summary() -> dict:
    return _get("/api/summary")


def fetch_metrics(minutes: int = 60) -> dict:
    return _get("/api/metrics", minutes=minutes, table="minute")


def fetch_alerts(limit: int = 50) -> dict:
    return _get("/api/alerts", limit=limit)


def fetch_health() -> dict:
    with httpx.Client(base_url=BACKEND_URL, timeout=5.0) as client:
        response = client.get("/api/health")
        response.raise_for_status()
        return response.json()