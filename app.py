"""
Telemetry Pipeline Cost Calculator — entry point.

Responsibilities of this file:
  1. Page config and global CSS
  2. DB initialisation and service data loading
  3. Session-state defaults (so widgets have sensible initial values)
  4. Sidebar controls (device slider, window picker, suggested-pipeline button)
  5. Top-level title
  6. Tab shell + delegation to tabs/builder.py, tabs/compare.py, tabs/scale.py

All chart and widget logic lives in the tabs/ package, keeping each file
under 600 lines and making individual tabs easy to find and edit.
"""

import streamlit as st

from database import init_db, get_services_by_category
from pricing import msgs_per_month, msgs_per_sec
from tabs import builder, compare, scale

# ──────────────────────────────────────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Telemetry Pipeline Cost Calculator",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    #MainMenu, footer { visibility: hidden; }
    [data-testid="stMetricLabel"] { font-size: 0.75rem; }
    /* Coloured category badge used in builder.py */
    .cat-badge {
        display: inline-block;
        padding: 0.15rem 0.55rem;
        border-radius: 6px;
        font-size: 0.78rem;
        font-weight: 600;
        margin-bottom: 0.4rem;
    }
    .badge-ingestion  { background:#FF6B6B33; color:#FF6B6B; border:1px solid #FF6B6B55; }
    .badge-processing { background:#4ECDC433; color:#4ECDC4; border:1px solid #4ECDC455; }
    .badge-storage    { background:#45B7D133; color:#45B7D1; border:1px solid #45B7D155; }
    .badge-analytics  { background:#96CEB433; color:#96CEB4; border:1px solid #96CEB455; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# DB init — @st.cache_resource means this runs once per interpreter process,
# not once per user interaction.
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def _init_db():
    init_db()

_init_db()

# ──────────────────────────────────────────────────────────────────────────────
# Load services — @st.cache_data caches the return value across reruns so the
# SQLite queries only fire when the cache is cold (first load or deploy restart).
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data
def _load_services() -> dict:
    return {
        cat: get_services_by_category(cat)
        for cat in ("ingestion", "processing", "storage", "analytics")
    }

svc = _load_services()

# ──────────────────────────────────────────────────────────────────────────────
# Session-state defaults
# Set before any widget is rendered so each key already has a value the first
# time a widget reads it.  The "Apply Suggested Pipeline" button overwrites
# these same keys to load the recommended stack in one click.
# ──────────────────────────────────────────────────────────────────────────────
_DEFAULTS: dict = {
    "ing":  "KDS + Firehose + KDA Flink",
    "proc": ["Delta Live Tables", "Databricks Workflows"],
    "stor": ["S3 Data Lake", "DynamoDB"],
    "ana":  "Databricks SQL Warehouse",
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ──────────────────────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Parameters")
    st.markdown("---")

    num_devices: int = st.slider(
        "Number of Devices",
        min_value=10_000,
        max_value=50_000_000,
        value=10_000_000,
        step=10_000,
        help="Total number of telemetry-emitting devices",
    )
    if num_devices >= 1_000_000:
        st.caption(f"📱 **{num_devices / 1_000_000:.2f} M** devices")
    else:
        st.caption(f"📱 **{num_devices / 1_000:.0f} K** devices")

    st.markdown("---")

    WINDOW_MAP: dict = {
        "1 min": 1, "5 min": 5, "10 min": 10, "15 min": 15, "20 min": 20,
    }
    window_label: str = st.radio(
        "Ingestion Window",
        options=list(WINDOW_MAP.keys()),
        index=1,
        help="How often each device pushes a telemetry record",
    )
    window_min: int = WINDOW_MAP[window_label]

    mpm = msgs_per_month(num_devices, window_min)
    mps = msgs_per_sec(num_devices, window_min)
    st.caption(f"📨 **{mps:,.0f}** msgs/sec")
    st.caption(f"📦 **{mpm / 1e9:.2f} B** msgs/month")

    st.markdown("---")

    # ── Suggested Pipeline ────────────────────────────────────────────────────
    # Writing to session_state keys here (before the tab widgets render in the
    # same script pass) is enough to update all four selectors immediately —
    # no st.rerun() required.
    st.markdown("**Suggested Pipeline**")
    st.caption(
        "KDS + Firehose + KDA Flink → "
        "Delta Live Tables + Databricks Workflows → "
        "S3 Data Lake + DynamoDB → Databricks SQL Warehouse"
    )
    if st.button("⚡ Apply Suggested Pipeline", use_container_width=True):
        st.session_state["ing"]  = "KDS + Firehose + KDA Flink"
        st.session_state["proc"] = ["Delta Live Tables", "Databricks Workflows"]
        st.session_state["stor"] = ["S3 Data Lake", "DynamoDB"]
        st.session_state["ana"]  = "Databricks SQL Warehouse"

    st.markdown("---")
    st.caption(
        "Base prices at **10 M devices / 5-min** window.  \n"
        "Estimates based on public AWS & Databricks pricing."
    )

# ──────────────────────────────────────────────────────────────────────────────
# Page title
# ──────────────────────────────────────────────────────────────────────────────
st.title("Telemetry Pipeline Cost Calculator")
st.markdown(
    "**AWS + Databricks** — Real-Time & ML Pipelines for IoT / Telemetry Data  \n"
    "Adjust devices and ingestion window in the sidebar, then build your pipeline below."
)
st.markdown("---")

# ──────────────────────────────────────────────────────────────────────────────
# Tabs — each tab is fully self-contained in its own module.
# builder.render() returns a state dict so scale.render() can reference the
# current service selections without re-rendering any widgets.
# ──────────────────────────────────────────────────────────────────────────────
tab_build, tab_compare, tab_scale = st.tabs(
    ["🏗️ Pipeline Builder", "📊 Service Comparison", "📈 Scaling Analysis"]
)

builder_state = builder.render(tab_build,   svc, num_devices, window_min, window_label)
compare.render(               tab_compare,  svc, num_devices, window_min, window_label)
scale.render(                 tab_scale,    svc, builder_state, num_devices, window_min, window_label)
