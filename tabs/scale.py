"""
Scaling Analysis tab.

Two line charts driven by the user's current pipeline selection
(passed in via the builder_state dict from tabs/builder.py):
  • Left  — Cost vs. Ingestion Window  (devices fixed)
  • Right — Cost vs. Number of Devices (window fixed)
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pricing import calculate_cost, format_cost

LINE_COLORS = {
    "Total":      "#FFD700",
    "Ingestion":  "#FF6B6B",
    "Processing": "#4ECDC4",
    "Storage":    "#45B7D1",
    "Analytics":  "#96CEB4",
}


def render(
    tab,
    svc: dict,
    state: dict,
    num_devices: int,
    window_min: int,
    window_label: str,
) -> None:
    """
    Draw the Scaling Analysis tab.

    Parameters
    ----------
    tab          : Streamlit tab object
    svc          : dict of service lists keyed by category
    state        : dict returned by tabs.builder.render() — contains the
                   user's current service selections and pre-computed costs
    num_devices  : current device count
    window_min   : ingestion window in minutes
    window_label : human-readable label for display
    """
    with tab:
        st.subheader("Scaling Analysis")
        st.caption(
            "Uses the pipeline built in the Builder tab. "
            "Change services there to update these charts."
        )

        def _total(d: int, w: int) -> dict:
            """
            Compute cost breakdown for a given (devices, window) pair.
            Sums over all selected processing and storage services so
            multi-select choices are correctly reflected in the curves.
            """
            proc = sum(
                calculate_cost(
                    next(s for s in svc["processing"] if s["name"] == n), d, w
                )
                for n in state["sel_proc"]
            )
            stor = sum(
                calculate_cost(
                    next(s for s in svc["storage"] if s["name"] == n), d, w
                )
                for n in state["sel_storage"]
            )
            return {
                "Ingestion":  calculate_cost(state["ing_data"],  d, w),
                "Processing": proc,
                "Storage":    stor,
                "Analytics":  calculate_cost(state["ana_data"],  d, w),
            }

        col_l, col_r = st.columns(2, gap="medium")

        # ── LEFT: Cost vs. Ingestion Window ──────────────────────────────────
        # The curve data is fixed at `num_devices` devices (so it shifts when
        # the devices slider moves).  The "you are here" vertical line is
        # anchored to `window_min`, so it moves when the user changes the
        # window radio — making THIS chart visibly respond to its own control.
        with col_l:
            st.markdown("#### Cost vs. Ingestion Window")
            st.caption(f"Fixed: {num_devices/1e6:.2f} M devices — dashed line = current selection")

            windows = [1, 5, 10, 15, 20]
            rows_w = []
            for w in windows:
                parts = _total(num_devices, w)
                rows_w.append({"Window": w, **parts, "Total": sum(parts.values())})
            df_win = pd.DataFrame(rows_w)

            fig_win = go.Figure()
            for series, color in LINE_COLORS.items():
                fig_win.add_trace(
                    go.Scatter(
                        x=df_win["Window"],
                        y=df_win[series],
                        name=series,
                        line=dict(
                            color=color,
                            dash="solid" if series == "Total" else "dot",
                            width=3 if series == "Total" else 1.5,
                        ),
                        mode="lines+markers",
                        hovertemplate=(
                            f"<b>{series}</b><br>Window: %{{x}} min<br>"
                            f"Cost: $%{{y:,.0f}}<extra></extra>"
                        ),
                    )
                )
            # Vertical marker — moves when the sidebar window radio changes.
            # annotation_* kwargs are omitted: newer Plotly forwards them to
            # the Shape validator which rejects font_color as an unknown prop.
            fig_win.add_vline(
                x=window_min,
                line_width=2,
                line_dash="dash",
                line_color="rgba(255,255,255,0.5)",
            )
            fig_win.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="white",
                xaxis=dict(
                    title="Ingestion Window (min)",
                    gridcolor="#2d3250",
                    tickmode="array",
                    tickvals=[1, 5, 10, 15, 20],
                    ticktext=["1 m", "5 m", "10 m", "15 m", "20 m"],
                ),
                yaxis=dict(
                    title="Monthly Cost (USD)",
                    gridcolor="#2d3250",
                    tickprefix="$",
                    tickformat=",.0f",
                ),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                height=400,
                margin=dict(t=20),
            )
            st.plotly_chart(fig_win, use_container_width=True)

        # ── RIGHT: Cost vs. Number of Devices ─────────────────────────────────
        # The curve data is fixed at `window_min` (so it shifts when the window
        # radio changes).  The "you are here" vertical line is anchored to
        # `num_devices`, so it moves when the devices slider moves — making
        # THIS chart visibly respond to its own control.
        with col_r:
            st.markdown("#### Cost vs. Number of Devices")
            st.caption(f"Fixed: {window_label} — dashed line = current selection")

            device_range = [
                10_000, 50_000, 100_000, 500_000, 1_000_000,
                2_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000,
            ]
            rows_d = []
            for d in device_range:
                parts = _total(d, window_min)
                rows_d.append({"Devices": d, **parts, "Total": sum(parts.values())})
            df_dev = pd.DataFrame(rows_d)

            fig_dev = go.Figure()
            for series, color in LINE_COLORS.items():
                fig_dev.add_trace(
                    go.Scatter(
                        x=df_dev["Devices"],
                        y=df_dev[series],
                        name=series,
                        line=dict(
                            color=color,
                            dash="solid" if series == "Total" else "dot",
                            width=3 if series == "Total" else 1.5,
                        ),
                        mode="lines+markers",
                        hovertemplate=(
                            f"<b>{series}</b><br>Devices: %{{x:,}}<br>"
                            f"Cost: $%{{y:,.0f}}<extra></extra>"
                        ),
                    )
                )
            # Vertical marker — moves when the sidebar devices slider changes.
            fig_dev.add_vline(
                x=num_devices,
                line_width=2,
                line_dash="dash",
                line_color="rgba(255,255,255,0.5)",
            )
            fig_dev.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="white",
                xaxis=dict(
                    title="Number of Devices",
                    gridcolor="#2d3250",
                    type="log",
                    tickformat=",",
                ),
                yaxis=dict(
                    title="Monthly Cost (USD)",
                    gridcolor="#2d3250",
                    tickprefix="$",
                    tickformat=",.0f",
                ),
                legend=dict(bgcolor="rgba(0,0,0,0)"),
                height=400,
                margin=dict(t=20),
            )
            st.plotly_chart(fig_dev, use_container_width=True)
