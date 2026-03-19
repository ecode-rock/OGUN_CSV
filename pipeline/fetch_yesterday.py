#!/usr/bin/env python3
"""
fetch_yesterday.py — OGUN_CSV Daily Scraper
Runs via GitHub Actions every morning to fetch all Final games from yesterday
and append them to the appropriate monthly CSV in data/.

Duplicate-safe: skips any play_id already present in the CSV.
Handles off-season gracefully (no games = clean exit, not an error).

Usage:
    python pipeline/fetch_yesterday.py
    python pipeline/fetch_yesterday.py --date 2025-09-15  # override date
"""

import argparse
import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

# ── Paths ──────────────────────────────────────────────────────────────────────
ROOT     = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── API ────────────────────────────────────────────────────────────────────────
SCHEDULE_URL = "https://baseballsavant.mlb.com/schedule?date={year}-{month}-{day}"
GAME_URL     = "https://baseballsavant.mlb.com/gf?game_pk={game_pk}"
GAME_DELAY   = 1.5

# ── Column Whitelist (mirrors fetch_range.py) ──────────────────────────────────
WHITELIST = [
    "game_pk", "game_date", "home_team", "away_team", "type", "play_id",
    "inning", "ab_number", "cap_index", "outs",
    "batter", "stand", "batter_name",
    "pitcher", "p_throws", "pitcher_name",
    "team_batting", "team_fielding", "team_batting_id", "team_fielding_id",
    "result", "des", "events", "contextMetrics",
    "strikes", "balls", "pre_strikes", "pre_balls",
    "call", "call_name", "pitch_call", "is_strike_swinging",
    "result_code",
    "pitch_type", "pitch_name", "description",
    "start_speed", "end_speed",
    "sz_top", "sz_bot",
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ", "breakZ",
    "px", "pz", "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth",
    "savantIsInZone", "isInZone", "isSword", "is_bip_out", "is_abs_challenge",
    "plate_x", "plate_z",
    "pitch_number", "player_total_pitches", "player_total_pitches_pitch_types",
    "pitcher_pa_number", "pitcher_time_thru_order", "game_total_pitches",
    "batSpeed", "hit_distance", "xba", "is_barrel", "hc_x_ft", "hc_y_ft",
    "hit_speed", "hit_angle", "launch_speed", "launch_angle",
    "runnerOn1B", "runnerOn2B", "runnerOn3B",
    "is_last_pitch",
    "double_header", "game_number",
]
NUMERIC_COLS = [
    "game_pk", "inning", "ab_number", "cap_index", "outs",
    "batter", "pitcher", "team_batting_id", "team_fielding_id",
    "strikes", "balls", "pre_strikes", "pre_balls",
    "start_speed", "end_speed", "sz_top", "sz_bot",
    "extension", "plateTime", "zone", "spin_rate",
    "breakX", "inducedBreakZ", "breakZ",
    "px", "pz", "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth", "plate_x", "plate_z",
    "pitch_number", "player_total_pitches", "player_total_pitches_pitch_types",
    "pitcher_pa_number", "pitcher_time_thru_order", "game_total_pitches",
    "batSpeed", "hit_distance", "hit_speed", "hit_angle",
    "hc_x_ft", "hc_y_ft", "launch_speed", "launch_angle", "game_number",
]
ROUND_2 = [
    "start_speed", "end_speed", "sz_top", "sz_bot", "extension", "plateTime",
    "spin_rate", "breakX", "inducedBreakZ", "breakZ",
    "pfxX", "pfxZ", "pfxZWithGravity", "pfxXWithGravity", "pfxXNoAbs",
    "plateTimeSZDepth", "batSpeed", "hit_distance", "hit_speed", "hit_angle",
    "hc_x_ft", "hc_y_ft", "launch_speed", "launch_angle", "xba",
]
ROUND_4 = ["plate_x", "plate_z", "px", "pz"]
BOOL_COLS = ["is_strike_swinging", "savantIsInZone", "isInZone", "isSword", "is_abs_challenge"]

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(ROOT / "edge_cases.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ── Helpers (shared with fetch_range.py) ──────────────────────────────────────

def fetch_schedule(d: date) -> list[dict]:
    url = SCHEDULE_URL.format(year=d.year, month=d.month, day=d.day)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("SCHEDULE_FETCH_ERROR  date=%s  %s", d, exc)
        return []

    if not isinstance(data, dict):
        return []
    dates = data.get("schedule", {}).get("dates", [])
    if not dates:
        return []
    games = dates[0].get("games", [])
    if not games:
        return []

    final_games = []
    for g in games:
        game_pk   = g.get("gamePk")
        status    = g.get("status", {}).get("detailedState", "")
        dh        = g.get("doubleHeader", "N")
        gnum      = g.get("gameNumber", 1)
        teams     = g.get("teams", {})
        home_abbr = teams.get("home", {}).get("team", {}).get("abbreviation", "?")
        away_abbr = teams.get("away", {}).get("team", {}).get("abbreviation", "?")
        tag = f"{away_abbr}@{home_abbr}"

        if status == "Final":
            final_games.append({
                "game_pk":       int(game_pk),
                "double_header": dh,
                "game_number":   int(gnum),
                "home_team":     home_abbr,
                "away_team":     away_abbr,
            })
        elif status == "Postponed":
            log.warning("POSTPONED   date=%s  game_pk=%s  %s", d, game_pk, tag)
        elif status == "Cancelled":
            log.warning("CANCELLED   date=%s  game_pk=%s  %s", d, game_pk, tag)
        elif "Progress" in status or "Live" in status:
            log.warning("INCOMPLETE  date=%s  game_pk=%s  %s  status=%s", d, game_pk, tag, status)
    return final_games


def fetch_game_pitches(game_meta: dict) -> list[dict]:
    game_pk = game_meta["game_pk"]
    url = GAME_URL.format(game_pk=game_pk)
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        log.error("GAME_FETCH_ERROR  game_pk=%s  %s", game_pk, exc)
        return []

    game_date = data.get("game_date", "")
    home_team = game_meta["home_team"]
    away_team = game_meta["away_team"]

    rows = []
    for side in ("home_pitchers", "away_pitchers"):
        pitcher_dict = data.get(side, {})
        if not isinstance(pitcher_dict, dict):
            continue
        for pitcher_id, pitch_list in pitcher_dict.items():
            if not isinstance(pitch_list, list):
                continue
            for item in pitch_list:
                if not isinstance(item, dict) or "play_id" not in item:
                    continue
                row = dict(item)
                row["game_date"]     = game_date
                row["home_team"]     = home_team
                row["away_team"]     = away_team
                row["double_header"] = game_meta["double_header"]
                row["game_number"]   = game_meta["game_number"]
                row["game_pk"]       = int(row.get("game_pk", game_pk))
                rows.append(row)

    log.info("  game_pk=%-8s  %s@%s  %d rows", game_pk, away_team, home_team, len(rows))
    return rows


def _context_metrics_to_str(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, dict):
        return None if not val else json.dumps(val)
    s = str(val).strip()
    return None if s in ("", "{}", "None") else s


def compute_is_last_pitch(df: pd.DataFrame) -> pd.Series:
    gtp = pd.to_numeric(df["game_total_pitches"], errors="coerce")
    max_gtp = gtp.groupby([df["game_pk"], df["ab_number"]]).transform("max")
    is_last = gtp == max_gtp
    if "type" in df.columns:
        is_last = is_last.where(~df["type"].eq("no_pitch"), other=pd.NA)
    return is_last


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["is_last_pitch"] = compute_is_last_pitch(df)
    if "contextMetrics" in df.columns:
        df["contextMetrics"] = df["contextMetrics"].apply(_context_metrics_to_str)

    cols_available = [c for c in WHITELIST if c in df.columns]
    df = df[cols_available].copy()

    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "game_pk"     in df.columns: df["game_pk"]     = df["game_pk"].astype("Int64")
    if "game_number" in df.columns: df["game_number"] = df["game_number"].astype("Int64")
    if "game_date"   in df.columns: df["game_date"]   = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    if "xba"         in df.columns: df["xba"]         = pd.to_numeric(df["xba"], errors="coerce")
    if "is_barrel"   in df.columns: df["is_barrel"]   = pd.to_numeric(df["is_barrel"], errors="coerce").astype("Int64")
    if "is_bip_out"  in df.columns: df["is_bip_out"]  = df["is_bip_out"].map({"Y": True, "N": False, True: True, False: False})

    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].map({True: True, False: False})
    if "is_last_pitch" in df.columns:
        df["is_last_pitch"] = df["is_last_pitch"].map({True: True, False: False, pd.NA: None})
    for col in ("runnerOn1B", "runnerOn2B", "runnerOn3B"):
        if col in df.columns:
            df[col] = df[col].map({True: True, False: False, None: None})

    for col in ROUND_2:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
    for col in ROUND_4:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)
    return df


def sort_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(
        ["game_pk", "game_total_pitches"], ascending=True, na_position="last"
    ).reset_index(drop=True)


def append_to_csv(df: pd.DataFrame) -> None:
    """Merge new rows into the monthly CSV. Deduplicates on play_id."""
    if df.empty:
        log.info("  No rows to write.")
        return

    df["_ym"] = pd.to_datetime(df["game_date"]).dt.to_period("M")
    for ym, chunk in df.groupby("_ym"):
        chunk = chunk.drop(columns=["_ym"])
        out_path = DATA_DIR / f"{ym.year}_{ym.month:02d}.csv"

        if out_path.exists():
            existing = pd.read_csv(out_path, dtype=str)
            before   = len(existing)
            combined = pd.concat([existing, chunk.astype(str)], ignore_index=True)
            combined = combined.drop_duplicates(subset=["play_id"], keep="first")
            new_rows = len(combined) - before
            if new_rows == 0:
                log.info("  %s — all %d rows already present, nothing to add.",
                         out_path.name, len(chunk))
                continue
            log.info("  %s — adding %d new rows (was %d, now %d)",
                     out_path.name, new_rows, before, len(combined))
        else:
            combined = chunk
            log.info("  %s — new file, writing %d rows", out_path.name, len(combined))

        combined.to_csv(out_path, index=False, encoding="utf-8")
        log.info("  Wrote %s", out_path)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Fetch yesterday's MLB games into CSV")
    parser.add_argument("--date", help="Override date (YYYY-MM-DD). Default: yesterday.")
    args = parser.parse_args()

    if args.date:
        target = date.fromisoformat(args.date)
    else:
        target = date.today() - timedelta(days=1)

    log.info("=" * 60)
    log.info("fetch_yesterday.py  |  target date: %s", target)
    log.info("=" * 60)

    # Fetch schedule
    log.info("Fetching schedule for %s ...", target)
    games = fetch_schedule(target)

    if not games:
        log.info("No Final games found for %s — nothing to do.", target)
        sys.exit(0)

    log.info("Found %d Final game(s). Fetching pitch data...", len(games))

    # Fetch pitch data
    all_rows: list[dict] = []
    for i, game_meta in enumerate(games, 1):
        log.info("[%d/%d] Fetching game_pk=%s", i, len(games), game_meta["game_pk"])
        rows = fetch_game_pitches(game_meta)
        all_rows.extend(rows)
        time.sleep(GAME_DELAY)

    log.info("Total raw rows: %d", len(all_rows))
    if not all_rows:
        log.info("No pitch data collected. Exiting.")
        sys.exit(0)

    df = pd.DataFrame(all_rows)
    log.info("Cleaning %d rows...", len(df))
    df = clean_dataframe(df)
    df = sort_dataframe(df)
    log.info("Clean complete. Shape: %s", df.shape)

    append_to_csv(df)
    log.info("Done.")


if __name__ == "__main__":
    main()
