"""
ogun_app.py — OGUN Race Visualizer (CSV Edition)
Streamlit app that reads monthly CSVs from data/ — no database required.
"""

import math
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OGUN Race",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Data loading (CSV) ────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).resolve().parent.parent / "data"

@st.cache_data(ttl=300)
def load_all_data() -> pd.DataFrame:
    """
    Load and concatenate all monthly CSVs from data/.
    Returns empty DataFrame with correct columns if no files found.
    Cached for 5 minutes — GitHub Actions commits refresh via Streamlit Cloud deploy.
    """
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    if not csv_files:
        return pd.DataFrame()

    frames = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            frames.append(df)
        except Exception as e:
            st.warning(f"Could not read {f.name}: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Normalise types
    combined["game_date"]  = pd.to_datetime(combined["game_date"], errors="coerce").dt.date
    combined["game_pk"]    = pd.to_numeric(combined["game_pk"], errors="coerce")
    combined["launch_speed"]  = pd.to_numeric(combined["launch_speed"], errors="coerce")
    combined["launch_angle"]  = pd.to_numeric(combined["launch_angle"], errors="coerce")
    combined["hit_distance"]  = pd.to_numeric(combined["hit_distance"], errors="coerce")
    combined["game_total_pitches"] = pd.to_numeric(combined["game_total_pitches"], errors="coerce")

    # is_last_pitch: stored as True/False string in CSV
    if "is_last_pitch" in combined.columns:
        combined["is_last_pitch"] = combined["is_last_pitch"].map(
            {True: True, False: False, "True": True, "False": False}
        )

    return combined


# ── Team colours ──────────────────────────────────────────────────────────────
TEAM_COLORS = {
    "ARI": "#A71930", "AZ":  "#A71930",
    "ATH": "#003831", "OAK": "#003831",
    "ATL": "#CE1141",
    "BAL": "#DF4601",
    "BOS": "#BD3039",
    "CHC": "#0E3386",
    "CWS": "#27251F",
    "CIN": "#C6011F",
    "CLE": "#00385D",
    "COL": "#333366",
    "DET": "#0C2340",
    "HOU": "#EB6E1F",
    "KC":  "#004687",
    "LAA": "#BA0021",
    "LAD": "#005A9C",
    "MIA": "#00A3E0",
    "MIL": "#12284B",
    "MIN": "#002B5C",
    "NYM": "#002D72",
    "NYY": "#003087",
    "PHI": "#E81828",
    "PIT": "#FDB827",
    "SD":  "#2F241D",
    "SEA": "#0C2C56",
    "SF":  "#FD5A1E",
    "STL": "#C41E3A",
    "TB":  "#092C5C",
    "TEX": "#003278",
    "TOR": "#134A8E",
    "WSH": "#AB0003",
}
NEUTRAL = "#888888"

def team_color(team: str | None) -> str:
    return TEAM_COLORS.get(team or "", NEUTRAL)


# ── View states ────────────────────────────────────────────────────────────────
VIEW_OPTIONS = ["ALL CONTACT", "HITS ONLY", "AGGREGATE"]
VIEW_LABELS  = {"ALL CONTACT": "ALL", "HITS ONLY": "HITS", "AGGREGATE": "AGG"}
HIT_EVENTS   = {"single", "double", "triple", "home run"}

def apply_view_filter(df: pd.DataFrame, view: str) -> pd.DataFrame:
    if df.empty or view != "HITS ONLY":
        return df
    mask = df["events"].str.lower().isin(HIT_EVENTS)
    return df[mask].reset_index(drop=True)


# ── Batted ball outcome classification ────────────────────────────────────────
OUTCOME_LABELS = ["HR", "3B", "2B", "1B", "FLY", "POP", "LINE", "GROUND", "OTHER"]

def classify_batted_ball(evt: str, la: float | None) -> str | None:
    e = (evt or "").lower()
    if e == "home run":   return "HR"
    if e == "triple":     return "3B"
    if e == "double":     return "2B"
    if e == "single":     return "1B"
    if e in ("pop out", "bunt pop out"):                         return "POP"
    if e in ("flyout", "sac fly", "sac fly double play"):        return "FLY"
    if e == "lineout":                                           return "LINE"
    if e in ("groundout", "forceout", "gidp", "double play",
             "bunt groundout", "sac bunt"):                      return "GROUND"
    if e in ("field error", "fielders choice", "fielders choice out"): return "OTHER"
    if e in ("field_out", "field out"):
        if la is None: return "FLY"
        if la >= 50:   return "POP"
        if la >= 25:   return "FLY"
        if la >= 10:   return "LINE"
        return "GROUND"
    if e in ("force_out", "double_play"): return "GROUND"
    if e == "home_run":                   return "HR"
    return None


# ── OGUN formula ──────────────────────────────────────────────────────────────
def calc_ogun(avg_dist: float, avg_ev: float, avg_la: float, optimum: float = 29) -> float | None:
    if not avg_ev:
        return None
    mult = np.cos(np.radians(abs(avg_la - optimum))) ** 2
    return (avg_dist / avg_ev) * mult

def ogun_color(score: float | None) -> str:
    if score is None: return "#888888"
    if score >= 2.0:  return "#00C851"
    if score >= 1.75: return "#9ACD32"
    if score >= 1.5:  return "#FFA500"
    return "#FF4444"

def ogun_label(score: float | None) -> str:
    if score is None: return "N/A"
    if score >= 2.0:  return "ELITE"
    if score >= 1.75: return "ABOVE AVG"
    if score >= 1.5:  return "BELOW AVG"
    return "POOR"


# ── CSV data helpers ──────────────────────────────────────────────────────────
def get_teams(df: pd.DataFrame) -> list[str]:
    if df.empty or "team_batting" not in df.columns:
        return []
    return sorted(df["team_batting"].dropna().unique().tolist())

def get_players(df: pd.DataFrame) -> list[str]:
    if df.empty or "batter_name" not in df.columns:
        return []
    return sorted(df["batter_name"].dropna().unique().tolist())

def get_available_dates(df: pd.DataFrame) -> list:
    if df.empty or "game_date" not in df.columns:
        return []
    return sorted(df["game_date"].dropna().unique().tolist())

def get_games_on_date(df: pd.DataFrame, game_date) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    mask = df["game_date"] == game_date
    return df[mask][["game_pk", "home_team", "away_team"]].drop_duplicates().sort_values("game_pk")

def fetch_abs(df: pd.DataFrame, mode: str, selector: str, date_mode: str,
              start_date=None, end_date=None, game_pk: int | None = None) -> pd.DataFrame:
    """
    Filter the master DataFrame to last-pitch rows for one panel.
    mode: 'TEAM' | 'PLAYER'
    selector: team abbrev or batter_name
    date_mode: 'DATE RANGE' | 'SINGLE GAME'
    """
    if df.empty or not selector:
        return pd.DataFrame()
    if date_mode == "DATE RANGE" and (start_date is None or end_date is None):
        return pd.DataFrame()
    if date_mode == "SINGLE GAME" and game_pk is None:
        return pd.DataFrame()

    cols_needed = [
        "ab_number", "inning", "game_date", "game_pk",
        "batter_name", "team_batting",
        "hit_distance", "launch_speed", "launch_angle",
        "events", "is_barrel", "game_total_pitches", "type", "is_last_pitch",
    ]
    available = [c for c in cols_needed if c in df.columns]
    sub = df[available].copy()

    # Filter to pitches with is_last_pitch = True
    if "type" in sub.columns:
        sub = sub[sub["type"] == "pitch"]
    if "is_last_pitch" in sub.columns:
        sub = sub[sub["is_last_pitch"] == True]

    # Mode filter
    if mode == "TEAM":
        sub = sub[sub["team_batting"] == selector]
    else:
        sub = sub[sub["batter_name"] == selector]

    # Date filter
    if date_mode == "SINGLE GAME":
        sub = sub[sub["game_pk"] == int(game_pk)]
    else:
        sub = sub[
            (sub["game_date"] >= start_date) &
            (sub["game_date"] <= end_date)
        ]

    if "game_total_pitches" in sub.columns:
        sub = sub.sort_values(["game_date", "game_pk", "game_total_pitches"])

    return sub.reset_index(drop=True)


# ── Arc drawing ───────────────────────────────────────────────────────────────
def make_arc(x_start: float, dist: float, angle: float,
             color: str, opacity: float, name: str) -> list:
    angle_clamped = max(0.0, min(90.0, angle if angle is not None else 20.0))
    peak_h = dist * np.sin(np.radians(angle_clamped)) * 0.35
    n = 40
    t = np.linspace(0, 1, n)
    x0, y0_pt = x_start, 0.0
    x1, y1    = x_start + dist / 2, peak_h
    x2, y2    = x_start + dist, 0.0
    bx = (1-t)**2 * x0 + 2*(1-t)*t*x1 + t**2 * x2
    by = (1-t)**2 * y0_pt + 2*(1-t)*t*y1 + t**2 * y2

    fill_trace = go.Scatter(
        x=np.concatenate([bx, bx[::-1]]),
        y=np.concatenate([by, np.zeros(n)]),
        fill="toself", fillcolor=color,
        opacity=opacity * 0.25, line=dict(width=0),
        showlegend=False, hoverinfo="skip", name=name,
    )
    line_trace = go.Scatter(
        x=bx, y=by, mode="lines",
        line=dict(color=color, width=2),
        opacity=opacity, showlegend=False,
        hovertemplate=(
            f"<b>{name}</b><br>"
            f"Dist: {dist:.0f} ft<br>"
            f"Angle: {angle:.1f}°<br>"
            "<extra></extra>"
        ),
        name=name,
    )
    return [fill_trace, line_trace]


def _lane_layout(xmax: float) -> dict:
    pad = xmax * 0.2 if xmax > 0 else 200
    return dict(
        height=210,
        margin=dict(l=0, r=0, t=0, b=28),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#0d1117",
        xaxis=dict(
            range=[0, xmax + pad],
            showgrid=True,
            gridcolor="rgba(255,255,255,0.07)",
            tickfont=dict(color="#888", size=10),
            title=dict(text="Cumulative Distance (ft)", font=dict(color="#888", size=10)),
            zeroline=False,
        ),
        yaxis=dict(visible=False, range=[0, None]),
        showlegend=False,
    )


def build_race_figure(contact_abs: pd.DataFrame, color: str,
                      current_idx: int, shared_xmax: float) -> go.Figure:
    fig = go.Figure()
    if contact_abs.empty or current_idx == 0:
        fig.update_layout(**_lane_layout(shared_xmax))
        return fig

    visible  = contact_abs.iloc[:current_idx]
    n        = len(visible)
    x_cursor = 0.0

    for i, (_, row) in enumerate(visible.iterrows()):
        dist    = float(row["hit_distance"] or 0)
        angle   = float(row["launch_angle"]) if row["launch_angle"] is not None and pd.notna(row["launch_angle"]) else 20.0
        is_last = (i == n - 1)
        opacity = (0.35 + 0.65 * (i / max(n - 1, 1))) if n > 1 else 1.0
        traces  = make_arc(x_cursor, dist, angle, color, opacity,
                           f"{row['batter_name']} – {row.get('events', '')}")
        for t in traces:
            fig.add_trace(t)

        if is_last:
            fig.add_trace(go.Scatter(
                x=[x_cursor], y=[0], mode="text", text=["🏏"],
                textfont=dict(size=18), showlegend=False, hoverinfo="skip",
            ))
            peak_h = dist * np.sin(np.radians(max(0, angle))) * 0.35
            fig.add_shape(
                type="line",
                x0=x_cursor + dist, x1=x_cursor + dist,
                y0=0, y1=max(peak_h * 1.5, 20),
                line=dict(color=color, width=1.5, dash="dash"),
            )
            opt_angle = 29.0
            guide_len = dist * 1.2 if dist > 0 else 100
            guide_x   = x_cursor + guide_len * np.cos(np.radians(opt_angle))
            guide_y   = guide_len * np.sin(np.radians(opt_angle)) * 0.35
            fig.add_trace(go.Scatter(
                x=[x_cursor, guide_x], y=[0, guide_y], mode="lines",
                line=dict(color="rgba(255,255,255,0.25)", width=1, dash="dot"),
                showlegend=False, hoverinfo="skip",
            ))

        x_cursor += dist

    fig.update_layout(**_lane_layout(shared_xmax))
    return fig


def build_aggregate_figure(contact_abs: pd.DataFrame, color: str,
                            shared_xmax: float) -> go.Figure:
    fig = go.Figure()
    if contact_abs.empty:
        fig.update_layout(**_lane_layout(shared_xmax))
        return fig

    avg_dist  = float(contact_abs["hit_distance"].mean())
    avg_la    = float(contact_abs["launch_angle"].mean()) if contact_abs["launch_angle"].notna().any() else 20.0
    avg_ev    = float(contact_abs["launch_speed"].mean()) if contact_abs["launch_speed"].notna().any() else 0.0
    n_contact = len(contact_abs)

    angle_clamped = max(0.0, min(90.0, avg_la))
    peak_h = avg_dist * np.sin(np.radians(angle_clamped)) * 0.35
    n = 40
    t  = np.linspace(0, 1, n)
    bx = (1-t)**2 * 0 + 2*(1-t)*t*(avg_dist/2) + t**2 * avg_dist
    by = (1-t)**2 * 0 + 2*(1-t)*t*peak_h        + t**2 * 0

    hover = (
        f"<b>Aggregate · {n_contact} contact ABs</b><br>"
        f"Avg Dist: {avg_dist:.0f} ft<br>"
        f"Avg LA: {avg_la:.1f}°<br>"
        f"Avg EV: {avg_ev:.1f} mph<br>"
        "<extra></extra>"
    )
    fig.add_trace(go.Scatter(
        x=np.concatenate([bx, bx[::-1]]),
        y=np.concatenate([by, np.zeros(n)]),
        fill="toself", fillcolor=color, opacity=0.35,
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=bx, y=by, mode="lines",
        line=dict(color=color, width=4),
        opacity=1.0, showlegend=False,
        hovertemplate=hover, name="Aggregate",
    ))
    fig.add_shape(
        type="line",
        x0=avg_dist, x1=avg_dist,
        y0=0, y1=max(peak_h * 1.5, 20),
        line=dict(color=color, width=1.5, dash="dash"),
    )
    fig.update_layout(**_lane_layout(shared_xmax))
    return fig


# ── Stats ──────────────────────────────────────────────────────────────────────
def calc_stats(df: pd.DataFrame, up_to: int) -> dict:
    subset    = df.iloc[:up_to] if up_to > 0 else df.iloc[0:0]
    total_abs = len(subset)
    contact   = subset.dropna(subset=["launch_speed"])
    contact_abs = len(contact)

    avg_dist   = contact["hit_distance"].mean() if contact_abs else None
    avg_ev     = contact["launch_speed"].mean()  if contact_abs else None
    avg_la     = contact["launch_angle"].mean()  if contact_abs else None
    total_dist = contact["hit_distance"].sum()   if contact_abs else 0.0

    ogun = calc_ogun(avg_dist, avg_ev, avg_la) if (avg_dist and avg_ev and avg_la is not None) else None
    contact_rate = contact_abs / total_abs if total_abs else None

    return dict(
        total_dist=total_dist,
        avg_dist=avg_dist,
        avg_ev=avg_ev,
        avg_la=avg_la,
        ogun=ogun,
        contact_rate=contact_rate,
        total_abs=total_abs,
        contact_abs=contact_abs,
    )


# ── CSS ───────────────────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""
    <style>
    .stApp { background-color: #0d1117; color: #e6edf3; }
    [data-testid="stSidebar"] { background: #161b22; }
    .ogun-title {
        font-family: 'Arial Black', sans-serif;
        font-size: 1.7rem; font-weight: 900;
        letter-spacing: 0.12em; color: #e6edf3;
        margin-bottom: 0; line-height: 1.1;
    }
    .ogun-subtitle {
        font-size: 0.8rem; letter-spacing: 0.22em;
        color: #8b949e; margin-top: 0; text-transform: uppercase;
    }
    .panel-header {
        display: flex; align-items: center; gap: 14px;
        padding: 10px 0 6px 0;
        border-bottom: 1px solid #21262d; margin-bottom: 8px;
    }
    .panel-name { font-size: 1.15rem; font-weight: 700; letter-spacing: 0.06em; }
    .ogun-badge {
        display: inline-flex; flex-direction: column; align-items: center;
        padding: 4px 14px; border-radius: 8px; min-width: 80px;
    }
    .ogun-score { font-size: 1.5rem; font-weight: 900; line-height: 1.1; }
    .ogun-lbl   { font-size: 0.6rem; letter-spacing: 0.15em; opacity: 0.85; }
    .ogun-view-lbl { font-size: 0.55rem; letter-spacing: 0.12em; color: #8b949e; margin-top: 1px; }
    .stats-bar {
        display: flex; gap: 0; background: #161b22;
        border-radius: 6px; overflow: hidden; margin: 6px 0 4px 0;
    }
    .stat-cell {
        flex: 1; padding: 6px 4px; text-align: center;
        border-right: 1px solid #21262d;
    }
    .stat-cell:last-child { border-right: none; }
    .stat-val { font-size: 1.05rem; font-weight: 700; color: #e6edf3; }
    .stat-lbl { font-size: 0.6rem; color: #8b949e; letter-spacing: 0.1em; text-transform: uppercase; }
    .control-row {
        display: flex; align-items: center; gap: 12px;
        background: #161b22; padding: 10px 16px;
        border-radius: 8px; margin: 10px 0;
    }
    .pos-display {
        background: #21262d; border-radius: 6px;
        padding: 4px 12px; font-size: 0.85rem;
        color: #8b949e; white-space: nowrap;
    }
    .ab-log-wrap {
        max-height: 280px; overflow-y: auto;
        background: #0d1117; border: 1px solid #21262d; border-radius: 8px;
    }
    .ab-table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
    .ab-table th {
        background: #161b22; color: #8b949e; text-align: left;
        padding: 6px 10px; position: sticky; top: 0; z-index: 1;
        letter-spacing: 0.08em; font-size: 0.7rem; text-transform: uppercase;
        border-bottom: 1px solid #21262d;
    }
    .ab-table td { padding: 5px 10px; border-bottom: 1px solid #161b22; }
    .ab-table tr:last-child td { border-bottom: none; }
    .ab-row-current td { background: #1c2128 !important; }
    .ab-row-xbh td { color: #f0b429 !important; }
    .ab-row-hr td  { color: #f0b429 !important; font-weight: 700; }
    .panel-divider { border: none; border-top: 1px solid #21262d; margin: 16px 0; }
    .empty-state { color: #8b949e; font-size: 0.9rem; padding: 20px 0; text-align: center; }
    </style>
    """, unsafe_allow_html=True)


# ── Panel renderer ────────────────────────────────────────────────────────────
def render_panel(
    all_df: pd.DataFrame,
    col_key: str,            # 'left' | 'right'
    mode: str,               # 'TEAM' | 'PLAYER'
    selector: str,
    date_mode: str,
    start_date, end_date,
    game_pk: int | None,
    view: str,
    slider_val: int,
    shared_xmax: float,
) -> None:
    color = team_color(selector if mode == "TEAM" else None)

    abs_df = fetch_abs(
        all_df, mode, selector, date_mode,
        start_date, end_date, game_pk,
    )

    # View filter
    filtered = apply_view_filter(abs_df, view)
    contact  = filtered.dropna(subset=["launch_speed", "hit_distance"])
    contact  = contact[contact["hit_distance"] > 0].reset_index(drop=True)

    stats = calc_stats(abs_df, slider_val)
    view_stats = calc_stats(filtered, min(slider_val, len(filtered)))

    view_lbl = VIEW_LABELS.get(view, view)
    ogun_val = view_stats.get("ogun")
    ogun_c   = ogun_color(ogun_val)
    ogun_l   = ogun_label(ogun_val)
    ogun_str = f"{ogun_val:.3f}" if ogun_val is not None else "—"

    # Panel header
    st.markdown(
        f"""<div class="panel-header">
        <span class="panel-name" style="color:{color}">{selector or '—'}</span>
        <div class="ogun-badge" style="background:{ogun_c}22;border:1px solid {ogun_c}55">
            <span class="ogun-score" style="color:{ogun_c}">{ogun_str}</span>
            <span class="ogun-lbl"  style="color:{ogun_c}">{ogun_l}</span>
            <span class="ogun-view-lbl">{view_lbl}</span>
        </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # Stats bar
    def fmt(v, fmt_str, fallback="—"):
        try:
            return fmt_str.format(v) if v is not None and not math.isnan(float(v)) else fallback
        except Exception:
            return fallback

    cr = view_stats.get("contact_rate")
    cr_str = f"{cr:.0%}" if cr is not None else "—"
    st.markdown(
        f"""<div class="stats-bar">
        <div class="stat-cell">
            <div class="stat-val">{fmt(view_stats.get('avg_ev'),   '{:.1f}')}</div>
            <div class="stat-lbl">EV mph</div>
        </div>
        <div class="stat-cell">
            <div class="stat-val">{fmt(view_stats.get('avg_dist'), '{:.0f}')}</div>
            <div class="stat-lbl">Avg Dist ft</div>
        </div>
        <div class="stat-cell">
            <div class="stat-val">{fmt(view_stats.get('avg_la'),   '{:.1f}')}</div>
            <div class="stat-lbl">Avg LA °</div>
        </div>
        <div class="stat-cell">
            <div class="stat-val">{cr_str}</div>
            <div class="stat-lbl">Contact%</div>
        </div>
        <div class="stat-cell">
            <div class="stat-val">{fmt(view_stats.get('total_dist'), '{:.0f}')}</div>
            <div class="stat-lbl">Total Dist ft</div>
        </div>
        <div class="stat-cell">
            <div class="stat-val">{view_stats.get('contact_abs', 0)}/{view_stats.get('total_abs', 0)}</div>
            <div class="stat-lbl">Contact ABs</div>
        </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # Race figure
    n_contact = len(contact)
    view_contact = apply_view_filter(abs_df, view)
    view_contact = view_contact.dropna(subset=["launch_speed", "hit_distance"])
    view_contact = view_contact[view_contact["hit_distance"] > 0].reset_index(drop=True)
    n_show = min(slider_val, len(view_contact))

    if view == "AGGREGATE":
        fig = build_aggregate_figure(view_contact, color, shared_xmax)
    else:
        fig = build_race_figure(view_contact, color, n_show, shared_xmax)

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False}, key=f"chart_{col_key}")


# ── Main layout ───────────────────────────────────────────────────────────────
def main():
    inject_css()

    # Load all CSVs
    all_df = load_all_data()

    # Derive season year from most recent game_date in data, fall back to current year
    if not all_df.empty and "game_date" in all_df.columns:
        season_year = pd.to_datetime(all_df["game_date"].max()).year
    else:
        import datetime
        season_year = datetime.date.today().year

    # Title
    st.markdown(
        '<p class="ogun-title">⚾ OGUN RACE</p>'
        f'<p class="ogun-subtitle">Offensive Game Unifying Number · MLB {season_year}</p>',
        unsafe_allow_html=True,
    )

    if all_df.empty:
        st.error(
            "No data found in the `data/` folder. "
            "Run `pipeline/fetch_range.py` to load data first."
        )
        return

    available_dates = get_available_dates(all_df)
    teams   = get_teams(all_df)
    players = get_players(all_df)

    # ── Sidebar controls ──────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Controls")

        date_mode = st.radio("Date Mode", ["DATE RANGE", "SINGLE GAME"], index=0)

        if date_mode == "SINGLE GAME":
            sel_date = st.selectbox("Date", available_dates,
                                    index=len(available_dates) - 1 if available_dates else 0,
                                    format_func=str)
            games_on_date = get_games_on_date(all_df, sel_date)
            game_opts = {
                int(row["game_pk"]): f"{row['away_team']} @ {row['home_team']}"
                for _, row in games_on_date.iterrows()
            }
            sel_game_pk = st.selectbox(
                "Game",
                list(game_opts.keys()),
                format_func=lambda pk: game_opts.get(pk, str(pk)),
            ) if game_opts else None
            start_date = end_date = sel_date
            game_pk = sel_game_pk
        else:
            if available_dates:
                start_date = st.selectbox("Start Date", available_dates,
                                          index=0, format_func=str)
                end_date   = st.selectbox("End Date",   available_dates,
                                          index=len(available_dates) - 1, format_func=str)
            else:
                start_date = end_date = None
            game_pk = None

        st.divider()
        view = st.radio("View", VIEW_OPTIONS, index=0)

        st.divider()
        # Left panel
        st.subheader("Panel A")
        mode_l = st.radio("Mode", ["TEAM", "PLAYER"], key="mode_l")
        if mode_l == "TEAM":
            sel_l = st.selectbox("Team", teams, key="sel_l")
        else:
            sel_l = st.selectbox("Player", players, key="sel_l_p")

        st.divider()
        # Right panel
        st.subheader("Panel B")
        mode_r = st.radio("Mode", ["TEAM", "PLAYER"], key="mode_r")
        if mode_r == "TEAM":
            sel_r = st.selectbox("Team", teams, key="sel_r")
        else:
            sel_r = st.selectbox("Player", players, key="sel_r_p")

    # ── Shared AB slider ──────────────────────────────────────────────────────
    abs_l = fetch_abs(all_df, mode_l, sel_l, date_mode, start_date, end_date, game_pk)
    abs_r = fetch_abs(all_df, mode_r, sel_r, date_mode, start_date, end_date, game_pk)

    max_abs = max(len(abs_l), len(abs_r), 1)

    slider_val = st.slider(
        "At-Bats shown", min_value=0, max_value=max_abs, value=max_abs,
        step=1, key="slider",
    )

    # Shared X axis max
    def contact_total_dist(df_in, n):
        sub = df_in.iloc[:n].dropna(subset=["launch_speed", "hit_distance"])
        sub = sub[sub["hit_distance"] > 0]
        return float(sub["hit_distance"].sum()) if not sub.empty else 0.0

    xmax_l = contact_total_dist(abs_l, slider_val)
    xmax_r = contact_total_dist(abs_r, slider_val)
    shared_xmax = max(xmax_l, xmax_r, 200.0)

    # ── Two-column race panels ────────────────────────────────────────────────
    col_l, col_r = st.columns(2)

    with col_l:
        render_panel(
            all_df, "left", mode_l, sel_l,
            date_mode, start_date, end_date, game_pk,
            view, slider_val, shared_xmax,
        )

    with col_r:
        render_panel(
            all_df, "right", mode_r, sel_r,
            date_mode, start_date, end_date, game_pk,
            view, slider_val, shared_xmax,
        )


if __name__ == "__main__":
    main()
