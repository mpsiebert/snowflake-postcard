#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         Snowflake Summit 2025 — Postcard Activation CLI                     ║
║         Send a postcard from Moscone Center to anywhere in the USA!         ║
╚══════════════════════════════════════════════════════════════════════════════╝

Usage:
    python cli_app.py

Requires a .env file with Snowflake credentials. See .env.example.
"""

import os
import sys
import time
import textwrap
from datetime import date
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

import snowflake.connector
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich.prompt import Prompt
from rich.rule import Rule
from rich.table import Table
from rich.live import Live
from rich.spinner import Spinner
from rich import box

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
ORIGIN_LAT  = 37.773059
ORIGIN_LON  = -122.411038
ORIGIN_CITY = "Moscone Center, San Francisco"
ORIGIN_ZIP  = "94103"

DB_TARGET = "SUMMIT_APP.POSTCARDS.postcard_entries"
ZIP_TABLE = 'FREE_ZIPCODES_DB.PUBLIC.ZIP_CODE_META_SHARE'

# Brand colors (Rich markup)
BLUE    = "bold bright_blue"
CYAN    = "bold cyan"
YELLOW  = "bold yellow"
GREEN   = "bold bright_green"
MAGENTA = "bold magenta"

console = Console()


# ─────────────────────────────────────────────────────────────────────────────
# Snowflake Connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and return an authenticated Snowflake connection from .env.
    Uses key-pair authentication to bypass MFA requirements.
    """
    load_dotenv()

    required = ["SF_ACCOUNT", "SF_USER", "SF_PRIVATE_KEY_PATH", "SF_ROLE", "SF_WAREHOUSE"]
    missing  = [k for k in required if not os.getenv(k)]
    if missing:
        console.print(f"[bold red]✗ Missing .env keys:[/] {', '.join(missing)}")
        sys.exit(1)

    # Load private key from file (no passphrase)
    key_path = os.getenv("SF_PRIVATE_KEY_PATH")
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

    conn = snowflake.connector.connect(
        account    = os.getenv("SF_ACCOUNT"),
        user       = os.getenv("SF_USER"),
        private_key= pkb,
        role       = os.getenv("SF_ROLE"),
        warehouse  = os.getenv("SF_WAREHOUSE"),
        database   = "SUMMIT_APP",
        schema     = "POSTCARDS",
        session_parameters={"QUERY_TAG": "postcard_cli"},
    )
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Display Helpers
# ─────────────────────────────────────────────────────────────────────────────

def print_banner():
    """Print the welcome banner."""
    banner = Text(justify="center")
    banner.append("  ✉  SNOWFLAKE SUMMIT 2025  ✉\n", style="bold bright_white on blue")
    banner.append("   Postcard Activation Station\n", style="bold cyan")
    banner.append(f"   From: {ORIGIN_CITY}\n", style="dim white")

    console.print()
    console.print(Panel(banner, border_style="bright_blue", padding=(1, 4)))
    console.print()


def print_section(title: str):
    console.print()
    console.print(Rule(f"[bold cyan]{title}[/]", style="bright_blue"))
    console.print()


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Look up ZIP code
# ─────────────────────────────────────────────────────────────────────────────

def lookup_zip(cursor, dest_zip: str) -> dict | None:
    """
    Query the marketplace zip-centroid table.
    Returns dict with lat, lon, city, state or None if not found.
    """
    sql = f"""
        SELECT
            LATITUDE,
            LONGITUDE,
            CITY   AS city,
            STATE  AS state
        FROM {ZIP_TABLE}
        WHERE ZIP_CODE = %(zip)s
        LIMIT 1
    """
    cursor.execute(sql, {"zip": dest_zip.strip()})
    row = cursor.fetchone()
    if not row:
        return None
    return {"lat": row[0], "lon": row[1], "city": row[2], "state": row[3]}


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Calculate geospatial distance & build flight path
# ─────────────────────────────────────────────────────────────────────────────

def calculate_distance_and_path(cursor, dest_lat: float, dest_lon: float) -> dict:
    """
    Use Snowflake geospatial functions to compute:
      • distance in miles (ST_DISTANCE on a sphere, converted from metres)
      • GeoJSON LineString for the arc path
    """
    sql = """
        SELECT
            -- ST_DISTANCE returns metres; 1609.344 m per mile
            ROUND(
                ST_DISTANCE(
                    ST_MAKEPOINT(%(origin_lon)s, %(origin_lat)s),
                    ST_MAKEPOINT(%(dest_lon)s,   %(dest_lat)s)
                ) / 1609.344,
                1
            ) AS distance_miles,

            -- LineString GeoJSON: [origin] → [destination]
            ST_MAKELINE(
                ST_MAKEPOINT(%(origin_lon)s, %(origin_lat)s),
                ST_MAKEPOINT(%(dest_lon)s,   %(dest_lat)s)
            ) AS flight_path
    """
    params = {
        "origin_lat": ORIGIN_LAT,
        "origin_lon": ORIGIN_LON,
        "dest_lat":   dest_lat,
        "dest_lon":   dest_lon,
    }
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return {"distance_miles": row[0], "flight_path": row[1]}


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Insert record
# ─────────────────────────────────────────────────────────────────────────────

def insert_postcard(cursor, dest_zip: str, zip_info: dict, geo: dict) -> int:
    """
    Insert a postcard entry and return the generated entry_id.
    Snowflake MVCC means concurrent INSERTs from 6 laptops are non-blocking.
    """
    sql = f"""
        INSERT INTO {DB_TARGET}
            (dest_zip, dest_city, dest_state, dest_lat, dest_lon,
             distance_miles, flight_path)
        SELECT
            %(zip)s, %(city)s, %(state)s, %(lat)s, %(lon)s,
            %(dist)s,
            ST_MAKELINE(
                ST_MAKEPOINT(%(origin_lon)s, %(origin_lat)s),
                ST_MAKEPOINT(%(dest_lon)s,   %(dest_lat)s)
            )
    """
    params = {
        "zip":        dest_zip,
        "city":       zip_info["city"],
        "state":      zip_info["state"],
        "lat":        zip_info["lat"],
        "lon":        zip_info["lon"],
        "dist":       geo["distance_miles"],
        "origin_lat": ORIGIN_LAT,
        "origin_lon": ORIGIN_LON,
        "dest_lat":   zip_info["lat"],
        "dest_lon":   zip_info["lon"],
    }
    cursor.execute(sql, params)

    # Retrieve the just-inserted ID
    cursor.execute("SELECT MAX(entry_id) FROM " + DB_TARGET)
    row = cursor.fetchone()
    return row[0] if row else -1


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Cortex AI Q&A
# ─────────────────────────────────────────────────────────────────────────────

def build_cortex_context(cursor) -> str:
    """
    Pull today's aggregate stats + state leaderboard to give Cortex
    accurate, grounded context for answering attendee questions.
    """
    # Aggregate stats
    cursor.execute("SELECT * FROM SUMMIT_APP.POSTCARDS.postcard_stats LIMIT 1")
    stats_row = cursor.fetchone()
    stats_cols = [d[0].lower() for d in cursor.description]
    stats = dict(zip(stats_cols, stats_row)) if stats_row else {}

    # Top 10 states
    cursor.execute("""
        SELECT dest_state, postcard_count, avg_distance_miles
        FROM SUMMIT_APP.POSTCARDS.state_leaderboard
        LIMIT 10
    """)
    leaderboard_rows = cursor.fetchall()
    leaderboard_text = "\n".join(
        f"  {r[0]}: {r[1]} postcards (avg {r[2]} mi)" for r in leaderboard_rows
    ) or "  No data yet."

    context = f"""
You are a fun, enthusiastic data analyst assistant at the Snowflake Summit 2025 booth.
Attendees are sending physical postcards from Moscone Center in San Francisco to destinations
across the USA. Today's live stats are:

- Total postcards sent: {stats.get('total_postcards', 0)}
- Total miles traveled: {stats.get('total_miles', 0):,}
- Unique states reached: {stats.get('unique_states', 0)}
- Unique zip codes reached: {stats.get('unique_zips', 0)}
- Farthest postcard: {stats.get('max_distance_miles', 0)} miles
- Top destination state: {stats.get('top_state', 'N/A')}
- Top destination city: {stats.get('top_city', 'N/A')}

State leaderboard (top 10):
{leaderboard_text}

Answer the attendee's question below in 2–3 sentences, with enthusiasm and emojis.
Be concise — this is a live event kiosk.
"""
    return context.strip()


def ask_cortex(cursor, question: str) -> str:
    """
    Call SNOWFLAKE.CORTEX.COMPLETE with the attendee's question + live context.
    Uses the mistral-large2 model (available in all Snowflake regions).
    """
    context = build_cortex_context(cursor)

    full_prompt = f"{context}\n\nAttendee question: {question}"

    # Escape single-quotes for the SQL string literal
    escaped = full_prompt.replace("'", "''")

    sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            '{escaped}'
        )
    """
    cursor.execute(sql)
    row = cursor.fetchone()
    return row[0] if row else "(No response from Cortex)"


# ─────────────────────────────────────────────────────────────────────────────
# Main Attendee Flow
# ─────────────────────────────────────────────────────────────────────────────

def run_postcard_flow(conn: snowflake.connector.SnowflakeConnection):
    """
    Single attendee flow: zip → lookup → insert → Cortex Q&A.
    Returns when the attendee types 'exit' or 'done'.
    """
    cursor = conn.cursor()

    # ── Prompt for zip code ──────────────────────────────────────────────────
    print_section("📬  Enter Your Destination")

    dest_zip = ""
    while not dest_zip:
        dest_zip = Prompt.ask(
            "[bold cyan]  Destination Zip Code[/] (e.g. 10001)",
            console=console
        ).strip()
        if not dest_zip.isdigit() or len(dest_zip) not in (5,):
            console.print("  [yellow]Please enter a valid 5-digit US zip code.[/]")
            dest_zip = ""

    # ── Look up zip (with spinner) ───────────────────────────────────────────
    console.print()
    with console.status("[cyan]Looking up your destination...[/]", spinner="dots"):
        zip_info = lookup_zip(cursor, dest_zip)

    if not zip_info:
        console.print(Panel(
            f"[bold red]✗ Zip code [white]{dest_zip}[/] not found.[/]\n"
            "  Please try another zip code.",
            border_style="red"
        ))
        cursor.close()
        return

    # ── Calculate distance ───────────────────────────────────────────────────
    with console.status("[cyan]Calculating distance with Snowflake geospatial...[/]", spinner="earth"):
        geo = calculate_distance_and_path(cursor, zip_info["lat"], zip_info["lon"])

    # ── Insert record ────────────────────────────────────────────────────────
    with console.status("[cyan]Saving your postcard to Snowflake...[/]", spinner="arrow3"):
        entry_id = insert_postcard(cursor, dest_zip, zip_info, geo)

    # ── 🎉 Success message ───────────────────────────────────────────────────
    dist   = geo["distance_miles"]
    city   = zip_info["city"].title()
    state  = zip_info["state"]

    success_text = Text(justify="center")
    success_text.append("🎉  Postcard Sent!  🎉\n\n", style="bold bright_white")
    success_text.append(
        f"Your postcard is traveling  {dist:,.1f} miles\n",
        style="bold yellow"
    )
    success_text.append(
        f"from San Francisco to  {city}, {state}  ✉\n",
        style="bold cyan"
    )
    success_text.append(f"\n(Entry #{entry_id} recorded in Snowflake)", style="dim white")

    console.print()
    console.print(Panel(success_text, border_style="bright_green", padding=(1, 4)))

    # ── Stats table ──────────────────────────────────────────────────────────
    with console.status("[cyan]Fetching live leaderboard...[/]", spinner="dots"):
        cursor.execute("""
            SELECT total_postcards, total_miles, top_state
            FROM SUMMIT_APP.POSTCARDS.postcard_stats
            LIMIT 1
        """)
        stats_row = cursor.fetchone()

    if stats_row:
        table = Table(
            title="[bold cyan]Live Stats — Today[/]",
            box=box.ROUNDED,
            border_style="bright_blue",
            show_header=True,
            header_style="bold white on blue"
        )
        table.add_column("📬 Total Postcards", justify="center")
        table.add_column("✈️  Total Miles",     justify="center")
        table.add_column("🏆 Top State",         justify="center")
        table.add_row(
            str(stats_row[0]),
            f"{stats_row[1]:,}",
            str(stats_row[2] or "—"),
        )
        console.print()
        console.print(table)

    # ── Cortex AI Q&A loop ───────────────────────────────────────────────────
    print_section("🤖  Ask Cortex AI")
    console.print(
        "  [cyan]Cortex AI[/] can answer questions about today's postcards.\n"
        "  [dim]Examples: 'What state is getting the most postcards?'\n"
        "            'How far is the farthest postcard?'\n"
        "  Type [bold]'done'[/] or [bold]'exit'[/] when finished.[/]"
    )
    console.print()

    while True:
        question = Prompt.ask("[bold magenta]  Your question[/]", console=console).strip()

        if question.lower() in ("exit", "done", "quit", ""):
            break

        with console.status("[magenta]Cortex is thinking...[/]", spinner="star"):
            answer = ask_cortex(cursor, question)

        answer_wrapped = textwrap.fill(answer.strip(), width=72)
        console.print()
        console.print(Panel(
            f"[bold white]{answer_wrapped}[/]",
            title="[bold magenta]🤖 Cortex AI[/]",
            border_style="magenta",
            padding=(1, 2)
        ))
        console.print()

    cursor.close()
    console.print()
    console.print(Rule("[dim]Thank you for participating! Enjoy the Summit 🏔️[/]", style="bright_blue"))
    console.print()


# ─────────────────────────────────────────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print_banner()

    # Connect once; reuse across attendees to avoid repeated auth overhead
    with console.status("[cyan]Connecting to Snowflake...[/]", spinner="dots"):
        try:
            conn = get_connection()
        except Exception as exc:
            console.print(f"[bold red]Connection failed:[/] {exc}")
            sys.exit(1)

    console.print("  [bright_green]✓ Connected to Snowflake[/]\n")

    # Continuous loop — reset after each attendee
    while True:
        try:
            run_postcard_flow(conn)
        except KeyboardInterrupt:
            console.print("\n[dim]Interrupted. Resetting for next attendee...[/]")

        again = Prompt.ask(
            "\n  [bold cyan]Ready for next attendee?[/] [dim](yes / quit)[/]",
            console=console,
            default="yes"
        ).strip().lower()

        if again in ("quit", "q", "no", "n"):
            conn.close()
            console.print("\n[bold blue]Goodbye! See you at the next Summit. 👋[/]\n")
            sys.exit(0)

        console.clear()
        print_banner()


if __name__ == "__main__":
    main()
