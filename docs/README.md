# OGUN_CSV — MLB Pitch Visualizer (CSV Architecture)

**No database required.** All data lives in monthly CSV files committed to this repo.
GitHub Actions scrapes Baseball Savant every morning and commits fresh data.
Streamlit Cloud reads directly from those CSVs and auto-deploys on each commit.

---

## Architecture

```
[GitHub Actions — 8 AM ET daily]
        │
        ▼
pipeline/fetch_yesterday.py
  → GET /schedule?date=YYYY-M-D   (Baseball Savant)
  → Filter Final games
  → GET /gf?game_pk=GAMEPK × N games
  → Clean + sort pitch data
  → Append to data/YYYY_MM.csv
        │
        ▼
git commit + push  →  Streamlit Cloud auto-deploys
        │
        ▼
app/ogun_app.py
  → pd.read_csv(data/*.csv)
  → OGUN formula applied at display time
  → Race visualizer in browser
```

---

## The OGUN Formula

**OGUN (Offensive Game Unifying Number)** — a contact quality metric combining three Statcast inputs.

```
OGUN = (avg_distance / avg_exit_velo) × cos²(|avg_launch_angle − 29|)
```

| Score | Contact Quality |
|-------|----------------|
| ≥ 2.0 | Elite |
| 1.75 – 2.0 | Above Average |
| 1.50 – 1.75 | Below Average |
| < 1.50 | Poor |

**Filter applied before calculation:**
```
type = 'pitch'  AND  launch_speed IS NOT NULL  AND  is_last_pitch = True
```

---

## Project Structure

```
C:\OGUN_CSV\
├── app/
│   └── ogun_app.py              ← Streamlit app (reads CSVs, no DB)
├── data/
│   ├── 2025_03.csv              ← Late March (Opening Day)
│   ├── 2025_04.csv
│   ├── …
│   └── 2025_09.csv              ← September sample (seeded)
├── pipeline/
│   ├── fetch_range.py           ← Backfill: any date range → monthly CSVs
│   └── fetch_yesterday.py       ← Daily: yesterday's games → append to CSV
├── .github/
│   └── workflows/
│       └── daily_scrape.yml     ← GitHub Actions cron (8 AM ET)
├── docs/
│   └── README.md                ← This file
├── .gitignore
└── requirements.txt
```

---

## Running Locally

```bash
# Install deps
pip install -r requirements.txt

# Run Streamlit app (uses data/ CSVs directly)
streamlit run app/ogun_app.py

# Fetch a backfill range (edit START_DATE / END_DATE in the script first)
python pipeline/fetch_range.py

# Fetch yesterday manually
python pipeline/fetch_yesterday.py

# Fetch a specific date
python pipeline/fetch_yesterday.py --date 2025-09-15
```

---

## Data Files

Monthly CSVs follow the naming convention `YYYY_MM.csv`.
Each file contains all pitch-level rows for that calendar month.
The app loads all CSVs in `data/` automatically — adding a new month requires no code changes.

**Schema:** 84 columns derived from the Baseball Savant `/gf` API endpoint.
Key columns: `play_id`, `game_pk`, `game_date`, `team_batting`, `batter_name`,
`pitcher_name`, `pitch_type`, `start_speed`, `launch_speed`, `launch_angle`,
`hit_distance`, `events`, `is_last_pitch`, `is_barrel`, `xba`, `batSpeed`.

---

## API Endpoints

| Purpose | URL |
|---------|-----|
| Schedule (all games for a date) | `https://baseballsavant.mlb.com/schedule?date=YYYY-M-D` |
| Game data (all pitches) | `https://baseballsavant.mlb.com/gf?game_pk=GAMEPK` |

- No authentication required
- 1.5s delay between `/gf` calls to be polite
- Season runs approximately late March through October

---

## GitHub Actions Deployment

1. Push this repo to GitHub
2. In repo **Settings → Secrets**, no secrets needed (public API, no credentials)
3. Actions run automatically at **8:00 AM ET** daily
4. To trigger manually: **Actions → Daily MLB Scrape → Run workflow**
5. Each successful run commits an updated `data/YYYY_MM.csv`

---

## Streamlit Cloud Deployment

1. Connect Streamlit Cloud to this GitHub repo
2. Set **Main file path** to `app/ogun_app.py`
3. Every git push (including Actions commits) triggers auto-redeploy
4. No secrets or environment variables required — CSVs are read from the repo

---

## Edge Cases

| Situation | Handling |
|-----------|---------|
| Postponed / Cancelled game | Logged to `edge_cases.log`, skipped |
| No games today (off-season) | Clean exit, no error |
| Duplicate play_id | Silently skipped on merge |
| Missing API fields | `.get()` with None default throughout |
| Doubleheaders | Both games processed, tagged with `game_number` |
