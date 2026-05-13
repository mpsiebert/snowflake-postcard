# ✉ Snowflake Summit 2025 — Say Hello from Summit!

Interactive experience where attendees write and address a physical postcard, then use Snowflake to maps its journey from Moscone Center to anywhere in the USA — powered by Snowflake geospatial functions, Cortex AI, and Streamlit. 

---

## Project Structure

```
postcard-summit/
├── init_db.sql        ← Run once in Snowflake to set up DB, table, views
├── cli_app.py         ← CLI app for 6 booth laptops
├── tv_map.py          ← Streamlit real-time map for the TV display
├── requirements.txt
├── .env.example       ← Copy to .env and fill in credentials
└── README.md
```

---

## Setup

### 1. Credentials

```bash
cp .env.example .env
# Edit .env with your Snowflake account/user/password/role/warehouse
```

Your role must have:
- `USAGE` on `ZIP_CODES_DB.POSTALADMIN` (Marketplace dataset)
- `ALL` on `SUMMIT_APP` (created by the SQL script)
- `USAGE` on `POSTCARD_WH`

### 2. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Initialize Snowflake

Run `init_db.sql` in a Snowflake worksheet (or via SnowSQL):

```bash
snowsql -f init_db.sql
```

---

## Running the Apps

### Booth Laptops (6×) — CLI

```bash
python cli_app.py
```

Each attendee:
1. Enters their destination zip code
2. Sees distance calculated via Snowflake `ST_DISTANCE`
3. Record inserted into `postcard_entries`
4. Can ask Cortex AI a question about today's data
5. Type `done` → next attendee

### TV Display — Streamlit Map

```bash
streamlit run tv_map.py
```

- Auto-refreshes every 4 seconds
- Shows KPI metrics: total postcards, total miles, top state
- Arc map from SF to all destinations
- State leaderboard + recent entries sidebar

---

## Snowflake Features Highlighted

| Feature | Where Used |
|---|---|
| `ST_MAKEPOINT` | Build point geometry from lat/lon |
| `ST_DISTANCE` | Great-circle distance in metres → miles |
| `ST_MAKELINE` | Build LineString flight path |
| `GEOGRAPHY` column type | Stores the arc path in `postcard_entries` |
| `SNOWFLAKE.CORTEX.COMPLETE` | AI Q&A grounded in live data |
| Marketplace data | `ZIP_CODES_DB.POSTALADMIN."zcr_usa_zip_centroids"` |
| MVCC (no row-locks) | Safe concurrent INSERTs from 6 laptops |

---

## Cortex Model

The CLI uses **`mistral-large2`** by default (widely available across regions). 
To change it, edit the model name in `cli_app.py → ask_cortex()`.

Available alternatives: `snowflake-arctic`, `llama3-70b`, `gemma-7b`

---

## Concurrency Note

Snowflake uses **MVCC (Multi-Version Concurrency Control)** — INSERT statements never block each other. All 6 laptops can write simultaneously without any application-level locking needed.
