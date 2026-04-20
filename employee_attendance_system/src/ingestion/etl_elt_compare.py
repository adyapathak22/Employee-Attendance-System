"""
src/ingestion/etl_elt_compare.py
TASK 10 — ETL vs ELT: Build both pipelines and compare performance
"""
import pandas as pd
import sqlite3
import time
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

DB_PATH = "data/attendance.db"
RAW_PATH = "data/raw/attendance.csv"


# ──────────────────────────────────────────────────────────────
# ETL Pipeline: Extract → Transform → Load
# Transform BEFORE loading into target
# ──────────────────────────────────────────────────────────────
def run_etl():
    log.info("\n── ETL Pipeline ────────────────────────────────────────")
    timings = {}

    # EXTRACT
    t = time.perf_counter()
    if not os.path.exists(RAW_PATH):
        log.warning("Raw CSV not found. Run generate_data.py first.")
        return {}
    df_raw = pd.read_csv(RAW_PATH, low_memory=False)
    timings["extract"] = round(time.perf_counter() - t, 4)
    log.info(f"  Extract: {len(df_raw):,} rows in {timings['extract']}s")

    # TRANSFORM (business logic applied BEFORE load)
    t = time.perf_counter()
    df = df_raw.copy()
    df["status"].fillna("Unknown", inplace=True)
    df["hours_worked"].fillna(0, inplace=True)
    df["hours_worked"] = df["hours_worked"].clip(0, 14)
    df["overtime_hours"] = df["overtime_hours"].clip(0, 4)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.dropna(subset=["date", "employee_id"], inplace=True)
    df["year_month"] = df["date"].dt.to_period("M").astype(str)
    df["is_weekend"] = df["date"].dt.weekday >= 5
    df["is_present"] = (df["status"] == "Present").astype(int)
    df["productivity_score"] = (df["hours_worked"] / 8).clip(0, 1.5).round(3)
    timings["transform"] = round(time.perf_counter() - t, 4)
    log.info(f"  Transform: {len(df):,} rows cleaned/enriched in {timings['transform']}s")

    # LOAD (clean, transformed data → target DB)
    t = time.perf_counter()
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql("etl_attendance_clean", conn, if_exists="replace", index=False)
    timings["load"] = round(time.perf_counter() - t, 4)
    log.info(f"  Load: → etl_attendance_clean in {timings['load']}s")

    total = sum(timings.values())
    log.info(f"  ETL Total: {total:.4f}s")
    return timings


# ──────────────────────────────────────────────────────────────
# ELT Pipeline: Extract → Load → Transform
# Load RAW data first, transform inside the database using SQL
# ──────────────────────────────────────────────────────────────
def run_elt():
    log.info("\n── ELT Pipeline ────────────────────────────────────────")
    timings = {}

    # EXTRACT
    t = time.perf_counter()
    if not os.path.exists(RAW_PATH):
        log.warning("Raw CSV not found.")
        return {}
    df_raw = pd.read_csv(RAW_PATH, low_memory=False)
    timings["extract"] = round(time.perf_counter() - t, 4)
    log.info(f"  Extract: {len(df_raw):,} rows in {timings['extract']}s")

    # LOAD (raw data as-is → staging table)
    t = time.perf_counter()
    with sqlite3.connect(DB_PATH) as conn:
        df_raw.to_sql("elt_attendance_raw", conn, if_exists="replace", index=False)
    timings["load"] = round(time.perf_counter() - t, 4)
    log.info(f"  Load: raw → elt_attendance_raw in {timings['load']}s")

    # TRANSFORM (SQL inside the database)
    t = time.perf_counter()
    transform_sql = """
    CREATE TABLE IF NOT EXISTS elt_attendance_clean AS
    SELECT
        attendance_id,
        employee_id,
        date,
        day_of_week,
        shift,
        COALESCE(status, 'Unknown') AS status,
        check_in,
        check_out,
        MIN(MAX(COALESCE(hours_worked, 0), 0), 14) AS hours_worked,
        MIN(MAX(COALESCE(overtime_hours, 0), 0), 4) AS overtime_hours,
        department,
        location,
        CASE WHEN status = 'Present' THEN 1 ELSE 0 END AS is_present,
        ROUND(MIN(MAX(COALESCE(hours_worked, 0) / 8.0, 0), 1.5), 3) AS productivity_score
    FROM elt_attendance_raw
    WHERE employee_id IS NOT NULL
      AND date IS NOT NULL;
    """
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DROP TABLE IF EXISTS elt_attendance_clean")
        conn.executescript(transform_sql)
        conn.commit()
    timings["transform"] = round(time.perf_counter() - t, 4)
    log.info(f"  Transform: SQL inside DB in {timings['transform']}s")

    total = sum(timings.values())
    log.info(f"  ELT Total: {total:.4f}s")
    return timings


def compare(etl_times, elt_times):
    log.info("\n── ETL vs ELT Comparison ───────────────────────────────")
    print(f"\n{'Phase':<12} {'ETL (s)':>10} {'ELT (s)':>10} {'Winner':>10}")
    print("-" * 46)
    for phase in ["extract", "transform", "load"]:
        e = etl_times.get(phase, 0)
        el = elt_times.get(phase, 0)
        winner = "ETL" if e < el else ("ELT" if el < e else "Tie")
        print(f"{phase:<12} {e:>10.4f} {el:>10.4f} {winner:>10}")

    etl_total = sum(etl_times.values())
    elt_total = sum(elt_times.values())
    print("-" * 46)
    print(f"{'TOTAL':<12} {etl_total:>10.4f} {elt_total:>10.4f} {'ETL' if etl_total < elt_total else 'ELT':>10}")

    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  ETL  → Best for: structured targets, strict schema,        ║
    ║         small/medium data, compliance-sensitive pipelines    ║
    ║  ELT  → Best for: cloud DW (BigQuery/Snowflake), raw        ║
    ║         storage flexibility, ad-hoc exploration, big data   ║
    ╚══════════════════════════════════════════════════════════════╝
    """)


def main():
    log.info("=== Task 10: ETL vs ELT Comparison ===\n")
    os.makedirs("data", exist_ok=True)
    etl_times = run_etl()
    elt_times = run_elt()
    if etl_times and elt_times:
        compare(etl_times, elt_times)
    log.info("✅ Task 10 Complete!")


if __name__ == "__main__":
    main()
