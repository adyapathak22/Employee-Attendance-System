"""
src/ingestion/batch_ingestor.py
TASK 11 — Data Ingestion: Batch ingestion pipeline from CSV to database
"""
import pandas as pd
import sqlite3
import os
import time
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

DB_PATH = "data/attendance.db"
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"


class BatchIngestor:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.stats = {
            "files_processed": 0, "rows_ingested": 0,
            "rows_failed": 0, "start_time": None, "end_time": None
        }

    def validate_schema(self, df: pd.DataFrame, table: str) -> bool:
        """Validate required columns exist before ingestion."""
        required = {
            "attendance": ["employee_id", "date", "status", "hours_worked"],
            "employees":  ["employee_id", "name", "department"],
            "departments": ["department_name"],
            "leaves": ["employee_id", "leave_type", "start_date"],
        }
        cols = required.get(table, [])
        missing = [c for c in cols if c not in df.columns]
        if missing:
            log.error(f"  Schema validation FAILED for {table}: missing {missing}")
            return False
        return True

    def ingest_file(self, filepath: str, table_name: str, mode: str = "append") -> int:
        """Ingest a single CSV file into the database."""
        if not Path(filepath).exists():
            log.warning(f"  File not found: {filepath}")
            return 0

        log.info(f"Ingesting {Path(filepath).name} → {table_name}...")
        start = time.perf_counter()

        try:
            df = pd.read_csv(filepath, low_memory=False)
            if df.empty:
                log.warning(f"  Empty file: {filepath}")
                return 0

            if not self.validate_schema(df, table_name):
                return 0

            # Basic pre-ingest cleaning
            df.dropna(how="all", inplace=True)
            df = df.where(pd.notnull(df), None)

            with sqlite3.connect(self.db_path) as conn:
                if_exists = "replace" if mode == "replace" else "append"
                df.to_sql(table_name, conn, if_exists=if_exists, index=False, chunksize=10_000)

            elapsed = round(time.perf_counter() - start, 3)
            throughput = round(len(df) / elapsed) if elapsed > 0 else 0
            log.info(f"  ✓ {len(df):,} rows in {elapsed}s ({throughput:,} rows/s)")

            self.stats["rows_ingested"] += len(df)
            self.stats["files_processed"] += 1
            return len(df)

        except Exception as e:
            log.error(f"  ✗ Ingestion failed for {filepath}: {e}")
            self.stats["rows_failed"] += 1
            return 0

    def run_full_batch(self):
        """Run complete batch ingestion pipeline."""
        self.stats["start_time"] = datetime.now()
        log.info("\n═══ Batch Ingestion Pipeline Started ═══\n")
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        jobs = [
            ("data/raw/departments.csv", "departments", "replace"),
            ("data/raw/employees.csv",   "employees",   "replace"),
            ("data/raw/attendance.csv",  "attendance",  "replace"),
            ("data/raw/leaves.csv",      "leaves",      "replace"),
        ]

        for filepath, table, mode in jobs:
            self.ingest_file(filepath, table, mode)

        # Create materialized summary view
        self._create_summary_tables()

        self.stats["end_time"] = datetime.now()
        self._print_report()

    def _create_summary_tables(self):
        """Create summary/aggregate tables post-ingestion."""
        log.info("\nCreating summary tables...")
        sqls = {
            "dept_attendance_summary": """
                SELECT department,
                       COUNT(*) as total_records,
                       SUM(CASE WHEN status='Present' THEN 1 ELSE 0 END) as present,
                       ROUND(AVG(hours_worked), 2) as avg_hours,
                       SUM(overtime_hours) as total_overtime
                FROM attendance
                GROUP BY department
            """,
            "monthly_trend": """
                SELECT strftime('%Y-%m', date) as month,
                       COUNT(DISTINCT employee_id) as active_employees,
                       SUM(CASE WHEN status='Present' THEN 1 ELSE 0 END) as present_count,
                       ROUND(AVG(hours_worked), 2) as avg_hours
                FROM attendance
                GROUP BY month
                ORDER BY month
            """,
        }
        with sqlite3.connect(self.db_path) as conn:
            for table_name, sql in sqls.items():
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    conn.execute(f"CREATE TABLE {table_name} AS {sql}")
                    conn.commit()
                    log.info(f"  Created: {table_name}")
                except Exception as e:
                    log.warning(f"  Failed to create {table_name}: {e}")

    def _print_report(self):
        duration = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        log.info(f"""
╔══════════════════════════════════════════════╗
║         Batch Ingestion Report               ║
╠══════════════════════════════════════════════╣
║  Files Processed : {self.stats['files_processed']:<5}                       ║
║  Rows Ingested   : {self.stats['rows_ingested']:<10,}                 ║
║  Rows Failed     : {self.stats['rows_failed']:<5}                       ║
║  Duration        : {duration:.2f}s                      ║
╚══════════════════════════════════════════════╝
""")


def main():
    log.info("=== Task 11: Batch Ingestion Pipeline ===")
    ingestor = BatchIngestor()
    ingestor.run_full_batch()
    log.info("✅ Task 11 Complete!")


if __name__ == "__main__":
    main()
