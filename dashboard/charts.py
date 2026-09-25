import pandas as pd
import plotly.graph_objects as go


def transactions_per_minute(metrics: dict) -> go.Figure:
    rows = metrics.get("rows", [])
    fig = go.Figure()
    if rows:
        df = pd.DataFrame(rows).sort_values("window_start")
        fig.add_trace(
            go.Scatter(
                x=df["window_start"],
                y=df["txn_count"],
                mode="lines+markers",
                name="Transactions",
                fill="tozeroy",
                line=dict(color="#4f8bf9", width=2),
                marker=dict(size=4, color="#4f8bf9"),
            )
        )
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(showgrid=True, gridcolor="rgba(128,128,128,0.15)")
    fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_title=None,
        yaxis_title="Transactions / min",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e1e1e1"),
    )
    return fig