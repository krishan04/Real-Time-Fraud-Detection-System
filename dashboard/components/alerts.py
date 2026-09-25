import pandas as pd
import streamlit as st

SEVERITY_COLORS = {
    "HIGH": "rgba(231, 76, 60, 0.25)",
    "MEDIUM": "rgba(243, 156, 18, 0.25)",
    "LOW": "rgba(128, 128, 128, 0.25)",
}


def _severity_bg(value: str) -> str:
    color = SEVERITY_COLORS.get(value, "rgba(128, 128, 128, 0.25)")
    return f"background-color: {color}"


def render(payload: dict) -> None:
    rows = payload.get("items", []) if payload else []
    if not rows:
        st.info("No fraud alerts yet — waiting for the first burst.")
        return

    df = pd.DataFrame(
        [
            {
                "Time": pd.to_datetime(row["detected_ts"]).strftime("%H:%M:%S"),
                "User": row["user_id"],
                "Amount": row["amount"],
                "Score": row["score"],
                "Severity": row["severity"],
                "Rules": " · ".join(row.get("reason") or []),
            }
            for row in rows
        ]
    )

    styled = (
        df.style.format({"Amount": "₹{:,.0f}", "Score": "{:.0%}"})
        .map(_severity_bg, subset=["Severity"])
    )

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=420,
    )