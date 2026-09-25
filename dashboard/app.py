import streamlit as st

from api import fetch_alerts, fetch_health, fetch_metrics, fetch_summary
from components import alerts, kpis, metrics as metrics_panel


def _status_dot(ok: bool) -> str:
    color = "#2ecc71" if ok else "#e74c3c"
    label = "LIVE" if ok else "OFFLINE"
    return f"<span style='color:{color};'>●</span> {label}"


@st.fragment(run_every=5)
def operations_panel() -> None:
    ok = True
    summary = None
    metrics = None
    try:
        health = fetch_health()
        ok = health.get("db", False)
        if ok:
            summary = fetch_summary()
            metrics = fetch_metrics(minutes=60)
    except Exception:
        ok = False

    st.markdown(
        "<div style='display:flex;justify-content:space-between;align-items:center;'>"
        "<div style='font-size:1.35rem;font-weight:700;'>REAL-TIME FRAUD DETECTION</div>"
        f"<div style='font-size:1.1rem;'>{_status_dot(ok)}</div></div>",
        unsafe_allow_html=True,
    )

    if not ok:
        st.warning("Dashboard API unreachable — retrying…")
        return

    kpis.render(summary)

    st.markdown("#### Transactions / Minute")
    metrics_panel.render(metrics)


@st.fragment(run_every=2)
def alerts_feed() -> None:
    payload = None
    try:
        payload = fetch_alerts(limit=50)
    except Exception:
        pass
    alerts.render(payload)


st.set_page_config(
    page_title="Real-Time Fraud Detection",
    page_icon="🛡️",
    layout="wide",
)

operations_panel()

st.markdown("#### Live Fraud Alerts")
alerts_feed()