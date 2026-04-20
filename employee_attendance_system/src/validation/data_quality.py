"""
src/validation/data_quality.py
TASK 29 — Data Quality: Validation checks, anomaly detection, profiling
"""
import pandas as pd
import numpy as np
import sqlite3
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    check_name: str
    table: str
    column: str
    passed: bool
    severity: str          # "critical", "warning", "info"
    expected: str
    actual: str
    row_count: int = 0
    timestamp: str = ""

    def __post_init__(self):
        self.timestamp = datetime.now().isoformat()


class AttendanceDataQuality:
    """Comprehensive data quality checks for the attendance system."""

    def __init__(self, db_path: str = "data/attendance.db"):
        self.db_path = db_path
        self.results: List[QualityCheckResult] = []

    def _load(self, table: str) -> pd.DataFrame:
        try:
            with sqlite3.connect(self.db_path) as conn:
                return pd.read_sql(f"SELECT * FROM {table}", conn)
        except Exception:
            csv_map = {
                "attendance": "data/raw/attendance.csv",
                "employees": "data/raw/employees.csv",
            }
            if table in csv_map and Path(csv_map[table]).exists():
                return pd.read_csv(csv_map[table], low_memory=False)
            return pd.DataFrame()

    def _record(self, name, table, col, passed, severity, expected, actual, row_count=0):
        result = QualityCheckResult(name, table, col, passed, severity, str(expected), str(actual), row_count)
        self.results.append(result)
        icon = "✓" if passed else ("✗" if severity == "critical" else "⚠")
        log.info(f"  [{icon}] {name}: {actual} (expected {expected})")
        return passed

    # ──────────────────────────────────────────────────────────
    # Completeness Checks
    # ──────────────────────────────────────────────────────────
    def check_completeness(self, df: pd.DataFrame, table: str):
        log.info(f"\n── Completeness Checks: {table} ──")
        critical_cols = {
            "attendance": ["employee_id", "date", "status"],
            "employees":  ["employee_id", "name", "department"],
        }.get(table, [])

        for col in critical_cols:
            if col not in df.columns:
                self._record(f"column_exists:{col}", table, col, False, "critical", "exists", "missing")
                continue
            null_pct = round(100 * df[col].isnull().sum() / len(df), 2)
            passed = null_pct == 0
            self._record(f"no_nulls:{col}", table, col, passed, "critical",
                         "0%", f"{null_pct}%", int(df[col].isnull().sum()))

    # ──────────────────────────────────────────────────────────
    # Validity Checks
    # ──────────────────────────────────────────────────────────
    def check_validity(self, df: pd.DataFrame, table: str):
        log.info(f"\n── Validity Checks: {table} ──")

        if table == "attendance":
            # Valid status values
            valid_statuses = {"Present", "Absent", "Late", "Half-Day", "Work-From-Home", "On-Leave", "Unknown"}
            if "status" in df.columns:
                invalid = ~df["status"].isin(valid_statuses)
                self._record("valid_status", table, "status",
                             invalid.sum() == 0, "critical",
                             f"in {valid_statuses}", f"{invalid.sum()} invalid",
                             int(invalid.sum()))

            # Hours worked range
            if "hours_worked" in df.columns:
                h = df["hours_worked"].dropna()
                out_of_range = ((h < 0) | (h > 14)).sum()
                self._record("hours_range", table, "hours_worked",
                             out_of_range == 0, "warning", "[0, 14]",
                             f"{out_of_range} violations", int(out_of_range))

            # Date validity
            if "date" in df.columns:
                future = (pd.to_datetime(df["date"], errors="coerce") > datetime.now()).sum()
                self._record("no_future_dates", table, "date",
                             future == 0, "warning", "≤ today",
                             f"{future} future dates", int(future))

        if table == "employees":
            if "salary" in df.columns:
                neg = (df["salary"] < 0).sum()
                self._record("positive_salary", table, "salary",
                             neg == 0, "critical", ">= 0",
                             f"{neg} negative salaries", int(neg))
            if "email" in df.columns:
                no_at = (~df["email"].str.contains("@", na=False)).sum()
                self._record("valid_email", table, "email",
                             no_at == 0, "warning", "contains @",
                             f"{no_at} invalid emails", int(no_at))

    # ──────────────────────────────────────────────────────────
    # Uniqueness Checks
    # ──────────────────────────────────────────────────────────
    def check_uniqueness(self, df: pd.DataFrame, table: str):
        log.info(f"\n── Uniqueness Checks: {table} ──")
        pk_map = {
            "attendance": ["employee_id", "date"],
            "employees": ["employee_id"],
        }
        pk_cols = pk_map.get(table, [])
        if all(c in df.columns for c in pk_cols):
            dupes = df.duplicated(subset=pk_cols).sum()
            self._record(f"unique_pk:{'+'.join(pk_cols)}", table, "+".join(pk_cols),
                         dupes == 0, "critical", "0 duplicates",
                         f"{dupes} duplicates", int(dupes))

    # ──────────────────────────────────────────────────────────
    # Referential Integrity
    # ──────────────────────────────────────────────────────────
    def check_referential_integrity(self, att_df: pd.DataFrame, emp_df: pd.DataFrame):
        log.info("\n── Referential Integrity ──")
        if "employee_id" in att_df.columns and "employee_id" in emp_df.columns:
            orphans = ~att_df["employee_id"].isin(emp_df["employee_id"])
            count = orphans.sum()
            self._record("fk_attendance→employees", "attendance", "employee_id",
                         count == 0, "critical", "all valid",
                         f"{count} orphan records", int(count))

    # ──────────────────────────────────────────────────────────
    # Anomaly Detection
    # ──────────────────────────────────────────────────────────
    def detect_anomalies(self, df: pd.DataFrame):
        log.info("\n── Anomaly Detection ──")

        if "hours_worked" in df.columns:
            hours = df["hours_worked"].dropna()
            # IQR-based outlier detection
            q1, q3 = hours.quantile(0.25), hours.quantile(0.75)
            iqr = q3 - q1
            outliers = ((hours < q1 - 3*iqr) | (hours > q3 + 3*iqr)).sum()
            self._record("hours_outliers", "attendance", "hours_worked",
                         outliers < len(df) * 0.01, "warning",
                         "<1% outliers", f"{outliers} outliers ({100*outliers/len(df):.2f}%)",
                         int(outliers))

        # Spike detection: day-level
        if "date" in df.columns and "status" in df.columns:
            daily = df.groupby("date")["employee_id"].count()
            mean = daily.mean()
            std = daily.std()
            spikes = (daily > mean + 3 * std).sum()
            self._record("daily_count_spikes", "attendance", "date",
                         spikes == 0, "info", "< mean+3σ",
                         f"{spikes} spike days", int(spikes))

        # Attendance rate drop
        if "status" in df.columns and "date" in df.columns:
            df_copy = df.copy()
            df_copy["date"] = pd.to_datetime(df_copy["date"], errors="coerce")
            df_copy["month"] = df_copy["date"].dt.to_period("M")
            monthly_rate = df_copy.groupby("month").apply(
                lambda x: (x["status"] == "Present").mean()
            )
            if len(monthly_rate) >= 2:
                drops = (monthly_rate.diff() < -0.15).sum()
                self._record("attendance_rate_drop", "attendance", "status",
                             drops == 0, "warning",
                             "MoM drop < 15%", f"{drops} months with >15% drop", int(drops))

    # ──────────────────────────────────────────────────────────
    # Run all checks and report
    # ──────────────────────────────────────────────────────────
    def run_all(self):
        log.info("=== Task 29: Data Quality Checks ===\n")
        os.makedirs("data/reports", exist_ok=True)

        att_df = self._load("attendance")
        emp_df = self._load("employees")

        if att_df.empty:
            log.error("No attendance data. Run generate_data.py first!")
            return

        self.check_completeness(att_df, "attendance")
        self.check_validity(att_df, "attendance")
        self.check_uniqueness(att_df, "attendance")
        self.detect_anomalies(att_df)

        if not emp_df.empty:
            self.check_completeness(emp_df, "employees")
            self.check_validity(emp_df, "employees")
            self.check_uniqueness(emp_df, "employees")
            self.check_referential_integrity(att_df, emp_df)

        self._print_report()
        self._save_report()

    def _print_report(self):
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        critical_failures = [r for r in self.results if not r.passed and r.severity == "critical"]
        warnings = [r for r in self.results if not r.passed and r.severity == "warning"]

        print(f"""
╔══════════════════════════════════════════════════╗
║          DATA QUALITY REPORT                     ║
╠══════════════════════════════════════════════════╣
║  Total Checks   : {total:<5}                          ║
║  Passed         : {passed:<5} ({100*passed//total if total else 0}%)                    ║
║  Critical Fails : {len(critical_failures):<5}                          ║
║  Warnings       : {len(warnings):<5}                          ║
╚══════════════════════════════════════════════════╝
""")
        if critical_failures:
            print("Critical Failures:")
            for r in critical_failures:
                print(f"  ✗ [{r.table}] {r.check_name}: {r.actual} (expected: {r.expected})")

    def _save_report(self):
        report = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.passed),
                "failed": sum(1 for r in self.results if not r.passed),
            },
            "results": [asdict(r) for r in self.results],
        }
        path = "data/reports/dq_report.json"
        with open(path, "w") as f:
            json.dump(report, f, indent=2)
        log.info(f"Report saved → {path}")


def main():
    dq = AttendanceDataQuality()
    dq.run_all()
    log.info("✅ Task 29 Complete!")


if __name__ == "__main__":
    main()
