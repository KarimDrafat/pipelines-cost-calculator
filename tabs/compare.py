"""
Service Comparison tab.

Shows a grouped bar chart of all 12 services at the current device count
and ingestion window, plus per-category expandable detail tables.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pricing import calculate_cost, format_cost

COLORS_CAT = {
    "Ingestion":  "#FF6B6B",
    "Processing": "#4ECDC4",
    "Storage":    "#45B7D1",
    "Analytics":  "#96CEB4",
}


def render(tab, svc: dict, num_devices: int, window_min: int, window_label: str) -> None:
    """
    Draw the Service Comparison tab.

    Parameters
    ----------
    tab          : Streamlit tab object
    svc          : dict of service lists keyed by category
    num_devices  : current device count
    window_min   : ingestion window in minutes
    window_label : human-readable label for display
    """
    with tab:
        st.subheader("Service Cost Comparison")
        st.caption(
            f"At **{num_devices/1e6:.2f} M devices** and **{window_label}** ingestion window. "
            "All categories shown side-by-side."
        )

        # Build flat DataFrame of all services and their current-parameter cost
        rows = []
        for cat_key, cat_label in [
            ("ingestion",  "Ingestion"),
            ("processing", "Processing"),
            ("storage",    "Storage"),
            ("analytics",  "Analytics"),
        ]:
            for s in svc[cat_key]:
                cost = calculate_cost(s, num_devices, window_min)
                rows.append({
                    "Category":     cat_label,
                    "Service":      s["name"],
                    "cost_num":     cost,
                    "Monthly Cost": format_cost(cost),
                })

        df_all = pd.DataFrame(rows)

        # Grouped bar chart — one colour per category
        fig = go.Figure()
        for cat_label, color in COLORS_CAT.items():
            sub = df_all[df_all["Category"] == cat_label]
            fig.add_trace(
                go.Bar(
                    name=cat_label,
                    x=sub["Service"],
                    y=sub["cost_num"],
                    marker_color=color,
                    text=sub["Monthly Cost"],
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>%{text}/month<extra></extra>",
                )
            )
        fig.update_layout(
            barmode="group",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="white",
            xaxis=dict(gridcolor="#2d3250", tickangle=-25),
            yaxis=dict(
                gridcolor="#2d3250",
                tickprefix="$",
                tickformat=",.0f",
                title="Monthly Cost (USD)",
            ),
            legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h", y=1.08),
            height=500,
            margin=dict(t=60, b=80),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Per-category expandable tables (Notes column omitted — too verbose)
        for cat_label in COLORS_CAT:
            with st.expander(f"📋 {cat_label} — all options"):
                sub = (
                    df_all[df_all["Category"] == cat_label][["Service", "Monthly Cost"]]
                    .reset_index(drop=True)
                )
                st.dataframe(sub, hide_index=True, use_container_width=True)
