"""
Pipeline Builder tab.

render() draws the entire tab and returns a state dict so the Scaling
Analysis tab can reuse the user's current service selections without
re-querying the DB or duplicating widget state.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from pricing import calculate_cost, format_cost

# Colour palette shared across all tabs (also imported by compare / scale)
COLORS = {
    "Ingestion":  "#FF6B6B",
    "Processing": "#4ECDC4",
    "Storage":    "#45B7D1",
    "Analytics":  "#96CEB4",
}


def render(tab, svc: dict, num_devices: int, window_min: int, window_label: str) -> dict:
    """
    Draw the Pipeline Builder tab.

    Parameters
    ----------
    tab          : Streamlit tab object returned by st.tabs()
    svc          : dict of lists, keyed by category name, loaded from SQLite
    num_devices  : current device-count slider value
    window_min   : ingestion window in minutes
    window_label : human-readable label (e.g. "5 min") for display

    Returns
    -------
    dict with keys: sel_ing, ing_data, c_ing, sel_proc, c_proc,
                    sel_storage, c_stor, sel_ana, ana_data, c_ana, total_cost
    """
    with tab:
        st.subheader("Build Your Custom Pipeline")
        st.caption(
            "Select one service for Ingestion and Analytics.  "
            "Processing and Storage support multi-select."
        )

        ingestion_names  = [s["name"] for s in svc["ingestion"]]
        processing_names = [s["name"] for s in svc["processing"]]
        storage_names    = [s["name"] for s in svc["storage"]]
        analytics_names  = [s["name"] for s in svc["analytics"]]

        col1, col2, col3, col4 = st.columns(4, gap="medium")

        # ── Ingestion ─────────────────────────────────────────────────────────
        with col1:
            st.markdown(
                '<span class="cat-badge badge-ingestion">📥 INGESTION</span>',
                unsafe_allow_html=True,
            )
            sel_ing = st.radio(
                "ingestion", options=ingestion_names,
                label_visibility="collapsed", key="ing",
            )
            ing_data = next(s for s in svc["ingestion"] if s["name"] == sel_ing)
            c_ing = calculate_cost(ing_data, num_devices, window_min)
            st.metric("Monthly", format_cost(c_ing))

        # ── Processing (multi-select) ─────────────────────────────────────────
        # Multi-select mirrors real architectures where DLT and Workflows run
        # together; costs are summed across all selected services.
        with col2:
            st.markdown(
                '<span class="cat-badge badge-processing">⚙️ PROCESSING</span>',
                unsafe_allow_html=True,
            )
            sel_proc = st.multiselect(
                "processing", options=processing_names,
                key="proc", label_visibility="collapsed",
            )
            if not sel_proc:
                sel_proc = [processing_names[0]]
                st.session_state["proc"] = sel_proc
            c_proc = sum(
                calculate_cost(
                    next(s for s in svc["processing"] if s["name"] == n),
                    num_devices, window_min,
                )
                for n in sel_proc
            )
            st.metric("Monthly", format_cost(c_proc))

        # ── Storage (multi-select + recommendation) ───────────────────────────
        with col3:
            st.markdown(
                '<span class="cat-badge badge-storage">💾 STORAGE</span>'
                '<br><span style="color:#45B7D1;font-size:0.7rem;">'
                "★ recommended to use both options</span>",
                unsafe_allow_html=True,
            )
            sel_storage = st.multiselect(
                "storage", options=storage_names,
                key="stor", label_visibility="collapsed",
            )
            if not sel_storage:
                sel_storage = [storage_names[0]]
                st.session_state["stor"] = sel_storage
            c_stor = sum(
                calculate_cost(
                    next(s for s in svc["storage"] if s["name"] == n),
                    num_devices, window_min,
                )
                for n in sel_storage
            )
            st.metric("Monthly", format_cost(c_stor))

        # ── Analytics ─────────────────────────────────────────────────────────
        with col4:
            st.markdown(
                '<span class="cat-badge badge-analytics">📊 ANALYTICS</span>',
                unsafe_allow_html=True,
            )
            sel_ana = st.radio(
                "analytics", options=analytics_names,
                label_visibility="collapsed", key="ana",
            )
            ana_data = next(s for s in svc["analytics"] if s["name"] == sel_ana)
            c_ana = calculate_cost(ana_data, num_devices, window_min)
            st.metric("Monthly", format_cost(c_ana))

        st.markdown("---")
        total_cost = c_ing + c_proc + c_stor + c_ana

        # ── Total cost card ───────────────────────────────────────────────────
        _, card_col, _ = st.columns([1, 2, 1])
        with card_col:
            st.markdown(
                f"""
                <div style="text-align:center;padding:1.5rem;border-radius:12px;
                            background:linear-gradient(135deg,#1a1d2e,#252840);
                            border:1px solid #00d4aa55;">
                  <div style="color:#aaa;font-size:0.85rem;letter-spacing:0.1em;">
                    TOTAL MONTHLY PIPELINE COST
                  </div>
                  <div style="color:#00d4aa;font-size:2.6rem;font-weight:700;margin:0.3rem 0;">
                    {format_cost(total_cost)}
                  </div>
                  <div style="color:#888;font-size:0.8rem;">
                    {format_cost(total_cost * 12)} / year &nbsp;|&nbsp;
                    {num_devices/1e6:.1f} M devices &nbsp;|&nbsp; {window_label}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ── Donut + breakdown table ───────────────────────────────────────────
        st.markdown("")
        left, right = st.columns(2, gap="large")

        with left:
            fig_donut = go.Figure(
                go.Pie(
                    labels=list(COLORS.keys()),
                    values=[c_ing, c_proc, c_stor, c_ana],
                    hole=0.52,
                    marker_colors=list(COLORS.values()),
                    textinfo="label+percent",
                    textfont_size=13,
                    hovertemplate="<b>%{label}</b><br>%{value:$,.0f}/month<extra></extra>",
                )
            )
            fig_donut.update_layout(
                title="Cost Distribution",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="white",
                showlegend=False,
                height=320,
                margin=dict(t=40, b=10, l=10, r=10),
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        with right:
            st.markdown("#### Breakdown")
            df = pd.DataFrame([
                {"Category": "Ingestion",  "Service": sel_ing,               "Monthly": format_cost(c_ing)},
                {"Category": "Processing", "Service": " + ".join(sel_proc),  "Monthly": format_cost(c_proc)},
                {"Category": "Storage",    "Service": " + ".join(sel_storage),"Monthly": format_cost(c_stor)},
                {"Category": "Analytics",  "Service": sel_ana,               "Monthly": format_cost(c_ana)},
                {"Category": "TOTAL",      "Service": "—",                   "Monthly": format_cost(total_cost)},
            ])
            st.dataframe(df, hide_index=True, use_container_width=True)

        # ── Pipeline flow diagram ─────────────────────────────────────────────
        # Uses st.columns for layout so each box is a separate, small markdown
        # call.  The previous single-string flexbox approach used 8-digit RGBA
        # hex colours (#RRGGBBAA) which Streamlit's renderer rejects, causing
        # the visible error below the diagram.
        st.markdown("---")
        st.markdown("#### Your Pipeline Flow")

        # 5 content columns + 4 narrow arrow columns
        flow_cols = st.columns([2, 0.35, 2, 0.35, 2, 0.35, 2, 0.35, 2])
        c_dev, _, c_ing_col, _, c_proc_col, _, c_stor_col, _, c_ana_col = flow_cols

        def _box(col, icon, label, color, service_text, cost):
            cost_html = (
                f"<div style='color:#00d4aa;font-size:0.88rem;font-weight:700;"
                f"margin-top:0.35rem;'>{cost}/mo</div>"
                if cost else ""
            )
            with col:
                st.markdown(
                    f"<div style='border:1px solid {color};border-radius:10px;"
                    f"padding:0.7rem 0.6rem;text-align:center;height:100%;'>"
                    f"<div style='font-size:1.3rem;'>{icon}</div>"
                    f"<div style='color:{color};font-size:0.6rem;font-weight:700;"
                    f"letter-spacing:0.07em;margin:0.2rem 0;'>{label}</div>"
                    f"<div style='color:#ddd;font-size:0.72rem;line-height:1.3;'>{service_text}</div>"
                    f"{cost_html}</div>",
                    unsafe_allow_html=True,
                )

        def _arrow(col):
            with col:
                st.markdown(
                    "<p style='text-align:center;color:#555;font-size:1.4rem;"
                    "margin-top:1.4rem;'>&#9654;</p>",
                    unsafe_allow_html=True,
                )

        _box(c_dev,      "📱", "DEVICES",    "#aaaaaa",
             f"{num_devices/1e6:.1f}M<br>{window_label}", "")
        _arrow(flow_cols[1])
        _box(c_ing_col,  "📥", "INGESTION",  "#FF6B6B", sel_ing,                       format_cost(c_ing))
        _arrow(flow_cols[3])
        _box(c_proc_col, "⚙️", "PROCESSING", "#4ECDC4", "<br>".join(sel_proc),          format_cost(c_proc))
        _arrow(flow_cols[5])
        _box(c_stor_col, "💾", "STORAGE",    "#45B7D1", "<br>".join(sel_storage),       format_cost(c_stor))
        _arrow(flow_cols[7])
        _box(c_ana_col,  "📊", "ANALYTICS",  "#96CEB4", sel_ana,                        format_cost(c_ana))

    # Return state so Scaling Analysis can reuse selections without re-rendering
    return {
        "sel_ing":    sel_ing,
        "ing_data":   ing_data,
        "c_ing":      c_ing,
        "sel_proc":   sel_proc,
        "c_proc":     c_proc,
        "sel_storage": sel_storage,
        "c_stor":     c_stor,
        "sel_ana":    sel_ana,
        "ana_data":   ana_data,
        "c_ana":      c_ana,
        "total_cost": total_cost,
    }
