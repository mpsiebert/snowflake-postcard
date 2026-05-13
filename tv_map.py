"""
╔══════════════════════════════════════════════════════════════════════════════╗
║    Snowflake Summit 2025 — Postcard Activation — TV Display Map             ║
║    tv_map.py                                                                ║
║                                                                             ║
║    Run:  streamlit run tv_map.py                                            ║
║    Credentials: .env or ~/.streamlit/secrets.toml                           ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import time

import pandas as pd
import pydeck as pdk
import streamlit as st
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

# ─────────────────────────────────────────────────────────────────────────────
# Page Config — must be first Streamlit call
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Snowflake Summit — Postcard Map",
    page_icon="✉️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
ORIGIN_LAT  = 37.773059
ORIGIN_LON  = -122.411038
REFRESH_SECONDS = 4          # auto-refresh interval

SNOWFLAKE_BLUE   = "#29B5E8"
SNOWFLAKE_NAVY   = "#0D1B2A"
SNOWFLAKE_DARK   = "#061527"
SNOWFLAKE_ACCENT = "#00E5FF"
ARC_COLOR_ORIGIN = [41, 181, 232, 200]    # Snowflake blue, semi-transparent
ARC_COLOR_TARGET = [0, 229, 255, 220]     # cyan

# ─────────────────────────────────────────────────────────────────────────────
# Custom CSS — Dark mode, large-screen optimised
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700;900&display=swap');

  /* Global dark background */
  html, body, [data-testid="stAppViewContainer"] {{
    background-color: {SNOWFLAKE_DARK} !important;
    color: #E8F4FD !important;
    font-family: 'Inter', sans-serif !important;
  }}

  /* Toolbar / header */
  [data-testid="stHeader"] {{
    background-color: {SNOWFLAKE_DARK} !important;
  }}

  /* KPI metric cards */
  [data-testid="metric-container"] {{
    background: linear-gradient(135deg, #0a2340 0%, #0d2f50 100%) !important;
    border: 1px solid {SNOWFLAKE_BLUE}44 !important;
    border-radius: 16px !important;
    padding: 24px 32px !important;
    box-shadow: 0 4px 24px rgba(41,181,232,0.12) !important;
  }}

  /* KPI label */
  [data-testid="metric-container"] label {{
    color: {SNOWFLAKE_BLUE} !important;
    font-size: 0.85rem !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    font-weight: 600 !important;
  }}

  /* KPI value */
  [data-testid="metric-container"] [data-testid="stMetricValue"] {{
    color: #FFFFFF !important;
    font-size: 3rem !important;
    font-weight: 900 !important;
    line-height: 1.1 !important;
  }}

  /* KPI delta */
  [data-testid="stMetricDelta"] {{
    color: {SNOWFLAKE_ACCENT} !important;
    font-size: 0.95rem !important;
  }}

  /* Hide streamlit footer / hamburger */
  #MainMenu, footer, [data-testid="stToolbar"] {{ visibility: hidden; }}

  /* Title banner */
  .summit-title {{
    text-align: center;
    font-size: 2.6rem;
    font-weight: 900;
    background: linear-gradient(90deg, {SNOWFLAKE_BLUE}, {SNOWFLAKE_ACCENT});
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.25rem;
    letter-spacing: -0.02em;
  }}

  .summit-subtitle {{
    text-align: center;
    color: #8ab4d4;
    font-size: 1.05rem;
    margin-bottom: 1.5rem;
    letter-spacing: 0.06em;
  }}

  /* Leaderboard table */
  .leaderboard-header {{
    color: {SNOWFLAKE_BLUE};
    font-size: 0.8rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    font-weight: 700;
    margin-bottom: 0.5rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid {SNOWFLAKE_BLUE}44;
  }}

  .leaderboard-row {{
    display: flex;
    justify-content: space-between;
    padding: 6px 0;
    border-bottom: 1px solid #ffffff0d;
    font-size: 1rem;
    color: #cde8f7;
  }}

  .leaderboard-rank {{
    color: {SNOWFLAKE_ACCENT};
    font-weight: 700;
    min-width: 28px;
  }}

  /* Scrollbar dark */
  ::-webkit-scrollbar {{ width: 6px; }}
  ::-webkit-scrollbar-track {{ background: {SNOWFLAKE_DARK}; }}
  ::-webkit-scrollbar-thumb {{ background: {SNOWFLAKE_BLUE}55; border-radius: 3px; }}

  /* Pulsing live dot */
  @keyframes pulse {{
    0%   {{ box-shadow: 0 0 0 0  rgba(0,229,255,0.7); }}
    70%  {{ box-shadow: 0 0 0 10px rgba(0,229,255,0); }}
    100% {{ box-shadow: 0 0 0 0  rgba(0,229,255,0); }}
  }}
  .live-dot {{
    display: inline-block;
    width: 10px; height: 10px;
    background: {SNOWFLAKE_ACCENT};
    border-radius: 50%;
    animation: pulse 1.4s infinite;
    margin-right: 6px;
    vertical-align: middle;
  }}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# Snowflake connection (cached — one connection per Streamlit session)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def get_connection():
    """Return a persistent Snowflake connection using key-pair auth (no MFA)."""
    load_dotenv()

    def _get(key: str) -> str:
        return (
            st.secrets.get(key)
            or os.getenv(key)
            or ""
        )

    key_path = _get("SF_PRIVATE_KEY_PATH")
    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend(),
        )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        account    = _get("SF_ACCOUNT"),
        user       = _get("SF_USER"),
        private_key= pkb,
        role       = _get("SF_ROLE"),
        warehouse  = _get("SF_WAREHOUSE"),
        database   = "SUMMIT_APP",
        schema     = "POSTCARDS",
        session_parameters={"QUERY_TAG": "postcard_tv_map"},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Data fetching (TTL cached so each refresh re-queries Snowflake)
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def fetch_entries() -> pd.DataFrame:
    """Fetch all of today's postcard entries."""
    conn = get_connection()
    sql  = """
        SELECT
            entry_id,
            created_at,
            dest_zip,
            dest_city,
            dest_state,
            dest_lat,
            dest_lon,
            distance_miles
        FROM SUMMIT_APP.POSTCARDS.postcard_entries
        WHERE CAST(created_at AS DATE) = CURRENT_DATE()
        ORDER BY created_at DESC
    """
    return pd.read_sql(sql, conn)


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def fetch_stats() -> dict:
    """Fetch aggregated today-stats from the view."""
    conn = get_connection()
    sql  = "SELECT * FROM SUMMIT_APP.POSTCARDS.postcard_stats LIMIT 1"
    df   = pd.read_sql(sql, conn)
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    return {k.lower(): v for k, v in row.items()}


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def fetch_leaderboard() -> pd.DataFrame:
    """Fetch state leaderboard (top 10)."""
    conn = get_connection()
    sql  = """
        SELECT dest_state, postcard_count, avg_distance_miles
        FROM SUMMIT_APP.POSTCARDS.state_leaderboard
        LIMIT 10
    """
    return pd.read_sql(sql, conn)


# ─────────────────────────────────────────────────────────────────────────────
# PyDeck Map
# ─────────────────────────────────────────────────────────────────────────────

def build_map(df: pd.DataFrame) -> pdk.Deck:
    """
    Build a PyDeck deck with:
      • ArcLayer  — arcs from SF origin to each destination
      • ScatterplotLayer — destination dots
      • ScatterplotLayer — origin pin (Moscone Center)
    """
    # Arc data — one row per postcard
    arc_data = df[["dest_lat", "dest_lon", "dest_city", "dest_state", "distance_miles"]].copy()
    arc_data["origin_lat"] = ORIGIN_LAT
    arc_data["origin_lon"] = ORIGIN_LON

    arc_layer = pdk.Layer(
        "ArcLayer",
        data=arc_data,
        get_source_position=["origin_lon", "origin_lat"],
        get_target_position=["dest_lon", "dest_lat"],
        get_source_color=ARC_COLOR_ORIGIN,
        get_target_color=ARC_COLOR_TARGET,
        auto_highlight=True,
        width_min_pixels=1,
        width_scale=0.5,
        get_width=3,
        great_circle=True,       # geodesic arcs for realism
        pickable=True,
    )

    # Destination scatter dots
    scatter_dest = pdk.Layer(
        "ScatterplotLayer",
        data=arc_data,
        get_position=["dest_lon", "dest_lat"],
        get_color=ARC_COLOR_TARGET,
        get_radius=30000,
        radius_min_pixels=4,
        radius_max_pixels=18,
        pickable=True,
    )

    # Origin pin (Moscone Center)
    origin_df = pd.DataFrame([{
        "lon": ORIGIN_LON,
        "lat": ORIGIN_LAT,
        "label": "Moscone Center (SF)",
    }])
    origin_layer = pdk.Layer(
        "ScatterplotLayer",
        data=origin_df,
        get_position=["lon", "lat"],
        get_color=[255, 255, 255, 255],
        get_radius=50000,
        radius_min_pixels=8,
        radius_max_pixels=22,
        pickable=False,
    )

    view_state = pdk.ViewState(
        latitude=38.5,
        longitude=-96,
        zoom=3.2,
        pitch=30,
        bearing=0,
    )

    tooltip = {
        "html": (
            "<b>✉ {dest_city}, {dest_state}</b><br/>"
            "ZIP: {dest_zip}<br/>"
            "Distance: {distance_miles} miles"
        ),
        "style": {
            "backgroundColor": "#0d1b2a",
            "color": "#29B5E8",
            "fontSize": "14px",
            "fontFamily": "Inter, sans-serif",
            "border": "1px solid #29B5E8",
            "borderRadius": "8px",
        },
    }

    return pdk.Deck(
        layers=[arc_layer, scatter_dest, origin_layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/dark-v11",
        tooltip=tooltip,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main Layout
# ─────────────────────────────────────────────────────────────────────────────

def main():
    # ── Title Banner ─────────────────────────────────────────────────────────
    st.markdown(
        '<div class="summit-title">✉  Snowflake Summit 2025  ✉</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="summit-subtitle">'
        '<span class="live-dot"></span>'
        'LIVE · Postcard Activation · Moscone Center, San Francisco'
        '</div>',
        unsafe_allow_html=True,
    )

    # ── Fetch data ────────────────────────────────────────────────────────────
    try:
        df          = fetch_entries()
        stats       = fetch_stats()
        leaderboard = fetch_leaderboard()
    except Exception as e:
        st.error(f"⚠️ Snowflake connection error: {e}")
        st.stop()

    # ── KPI Metrics Row ───────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)

    total_postcards = int(stats.get("total_postcards", 0))
    total_miles     = int(stats.get("total_miles", 0))
    top_state       = str(stats.get("top_state", "—"))
    unique_states   = int(stats.get("unique_states", 0))

    col1.metric("📬 Postcards Sent",   f"{total_postcards:,}")
    col2.metric("✈️  Miles Traveled",   f"{total_miles:,}")
    col3.metric("🏆 Top State",         top_state)
    col4.metric("🗺️  States Reached",   f"{unique_states}")

    st.markdown("<br/>", unsafe_allow_html=True)

    # ── Map + Leaderboard side-by-side ────────────────────────────────────────
    map_col, side_col = st.columns([3, 1])

    with map_col:
        if df.empty:
            st.info("⏳ Waiting for the first postcard to be sent…")
        else:
            deck = build_map(df)
            st.pydeck_chart(deck, use_container_width=True)

    with side_col:
        # ── State Leaderboard ─────────────────────────────────────────────
        st.markdown(
            '<div class="leaderboard-header">🏅 State Leaderboard</div>',
            unsafe_allow_html=True,
        )
        if leaderboard.empty:
            st.markdown("<div style='color:#8ab4d4;'>No data yet.</div>", unsafe_allow_html=True)
        else:
            for i, row in leaderboard.iterrows():
                rank   = i + 1
                medal  = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank, f"#{rank}")
                state  = row.get("DEST_STATE") or row.get("dest_state", "—")
                count  = int(row.get("POSTCARD_COUNT") or row.get("postcard_count", 0))
                avg_mi = int(row.get("AVG_DISTANCE_MILES") or row.get("avg_distance_miles", 0))
                st.markdown(
                    f'<div class="leaderboard-row">'
                    f'  <span class="leaderboard-rank">{medal}</span>'
                    f'  <span style="flex:1; padding:0 8px">{state}</span>'
                    f'  <span style="color:#29B5E8;font-weight:700">{count} ✉</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

        st.markdown("<br/>", unsafe_allow_html=True)

        # ── Recent Entries ────────────────────────────────────────────────
        st.markdown(
            '<div class="leaderboard-header">🕐 Recent Postcards</div>',
            unsafe_allow_html=True,
        )
        if not df.empty:
            recent = df.head(8)
            for _, row in recent.iterrows():
                city  = str(row.get("DEST_CITY") or row.get("dest_city", "")).title()
                state = str(row.get("DEST_STATE") or row.get("dest_state", ""))
                miles = row.get("DISTANCE_MILES") or row.get("distance_miles", 0)
                st.markdown(
                    f'<div class="leaderboard-row">'
                    f'  <span>📍 {city}, {state}</span>'
                    f'  <span style="color:#00E5FF">{miles:,.0f} mi</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── Auto-refresh counter ──────────────────────────────────────────────────
    st.markdown("<br/>", unsafe_allow_html=True)
    ts = time.strftime("%H:%M:%S")
    st.markdown(
        f'<div style="text-align:center;color:#3a5a7a;font-size:0.78rem;letter-spacing:0.05em;">'
        f'Auto-refreshing every {REFRESH_SECONDS}s &nbsp;·&nbsp; Last update: {ts}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Trigger browser auto-rerun
    time.sleep(REFRESH_SECONDS)
    st.rerun()


if __name__ == "__main__" or True:
    main()
