import streamlit as st


def render(summary: dict) -> None:
    if not summary:
        st.info("No summary data available yet.")
        return
    txn_total = summary.get("transactions_total", 0)
    alert_total = summary.get("alerts_total", 0)
    high_total = summary.get("alerts_by_severity", {}).get("HIGH", 0)
    flagged = summary.get("transactions_flagged", 0)

    column_txn, column_alerts, column_high = st.columns(3)
    column_txn.metric("Transactions", f"{txn_total:,}", help=f"{flagged:,} flagged")
    column_alerts.metric("Fraud Alerts", f"{alert_total:,}")
    column_high.metric("High Risk", f"{high_total:,}")