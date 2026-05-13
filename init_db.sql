-- =============================================================================
--  Snowflake Summit 2025 — Postcard Activation
--  Database Initialization Script
--
--  Run this ONCE as a role with CREATE DATABASE privileges (e.g. SYSADMIN).
--  It is idempotent (IF NOT EXISTS everywhere).
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Database & Schema
-- ─────────────────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS SUMMIT_APP;

CREATE SCHEMA IF NOT EXISTS SUMMIT_APP.POSTCARDS;

USE SCHEMA SUMMIT_APP.POSTCARDS;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Warehouse (XS, auto-suspend in 60 s so it doesn't idle)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS POSTCARD_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  COMMENT        = 'Summit postcard activation warehouse';

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. Target Table — postcard_entries
--
--  Concurrency note: Snowflake uses MVCC (multi-version concurrency control)
--  and does NOT row-lock on INSERT, so 6 simultaneous writers are safe.
--
--  GEOGRAPHY type stores the GeoJSON LineString for the flight path arc.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUMMIT_APP.POSTCARDS.postcard_entries (
    entry_id        NUMBER AUTOINCREMENT PRIMARY KEY,   -- surrogate key
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dest_zip        VARCHAR(10)   NOT NULL,
    dest_city       VARCHAR(100),
    dest_state      VARCHAR(50),
    dest_lat        FLOAT,
    dest_lon        FLOAT,
    distance_miles  FLOAT,
    -- GeoJSON LineString: ORIGIN → DESTINATION (great-circle path for the arc)
    flight_path     GEOGRAPHY
);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Quick sanity-check query  (run manually to verify the marketplace table)
-- ─────────────────────────────────────────────────────────────────────────────
-- SELECT ZIP, LAT, LON, NAME, STATE
-- FROM ZIP_CODES_DB.POSTALADMIN."zcr_usa_zip_centroids"
-- WHERE ZIP = '94103'   -- Moscone Center / SF
-- LIMIT 5;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Helpful view — aggregated stats used by the Cortex AI context prompt
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW SUMMIT_APP.POSTCARDS.postcard_stats AS
SELECT
    COUNT(*)                                              AS total_postcards,
    ROUND(SUM(distance_miles), 0)                         AS total_miles,
    COUNT(DISTINCT dest_state)                            AS unique_states,
    COUNT(DISTINCT dest_zip)                              AS unique_zips,
    MAX(distance_miles)                                   AS max_distance_miles,
    -- Top destination state
    MODE(dest_state)                                      AS top_state,
    -- Top destination city
    MODE(dest_city || ', ' || dest_state)                 AS top_city
FROM SUMMIT_APP.POSTCARDS.postcard_entries
WHERE CAST(created_at AS DATE) = CURRENT_DATE();

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. State leaderboard view (used by Cortex context)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW SUMMIT_APP.POSTCARDS.state_leaderboard AS
SELECT
    dest_state,
    COUNT(*)                   AS postcard_count,
    ROUND(AVG(distance_miles)) AS avg_distance_miles
FROM SUMMIT_APP.POSTCARDS.postcard_entries
WHERE CAST(created_at AS DATE) = CURRENT_DATE()
GROUP BY dest_state
ORDER BY postcard_count DESC;

-- Done!
SELECT 'Database initialization complete ✓' AS status;
