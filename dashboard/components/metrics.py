import streamlit as st

from charts import transactions_per_minute


def render(metrics: dict) -> None:
    if not metrics or not metrics.get("rows"):
        st.info("No per-minute metrics yet — waiting for the first window to close.")
        return
    st.plotly_chart(
        transactions_per_minute(metrics),
        use_container_width=True,
        config={"displayModeBar": False},
    )