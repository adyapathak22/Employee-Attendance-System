"""
src/ingestion/csv_merger.py
TASK 3 — Python Basics: Read multiple CSV files, clean missing values, merge datasets
"""
import os
import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


def read_csv_files(directory: str) -> dict[str, pd.DataFrame]:
    """Read all CSV files from a directory into a dictionary."""
    frames = {}
    path = Path(directory)
    csv_files = list(path.glob("*.csv"))

    if not csv_files:
        log.warning(f"No CSV files found in {directory}. Run generate_data.py first.")
        return frames

    for f in csv_files:
        log.info(f"Reading: {f.name}")
        df = pd.read_csv(f, low_memory=False)
        frames[f.stem] = df
        log.info(f"  → {len(df)} rows × {len(df.columns)} columns | Missing: {df.isnull().sum().sum()}")

    return frames


def clean_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the attendance DataFrame:
    - Fill missing hours_worked based on status
    - Fill missing status with 'Unknown'
    - Remove duplicate attendance records
    - Parse and validate date column
    - Cap overtime at 4 hours
    """
    log.info(f"Cleaning attendance: {len(df)} rows | Nulls before: {df.isnull().sum().sum()}")

    df = df.copy()
    df.drop_duplicates(subset=["employee_id", "date"], keep="first", inplace=True)

    # Fill missing status
    df["status"].fillna("Unknown", inplace=True)

    # Fill missing hours_worked based on status rules
    status_hours_map = {
        "Present": 8.0, "Late": 7.0, "Half-Day": 4.0,
        "Work-From-Home": 8.0, "Absent": 0.0, "On-Leave": 0.0, "Unknown": 0.0
    }
    for status, default_hours in status_hours_map.items():
        mask = (df["status"] == status) & (df["hours_worked"].isna())
        df.loc[mask, "hours_worked"] = default_hours

    # Cap overtime
    df["overtime_hours"] = df["overtime_hours"].clip(upper=4.0)

    # Validate hours range
    df["hours_worked"] = df["hours_worked"].clip(lower=0.0, upper=14.0)

    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    invalid_dates = df["date"].isna().sum()
    if invalid_dates > 0:
        log.warning(f"  Dropping {invalid_dates} rows with invalid dates")
        df.dropna(subset=["date"], inplace=True)

    log.info(f"  Nulls after: {df.isnull().sum().sum()} | Rows: {len(df)}")
    return df


def clean_employees(df: pd.DataFrame) -> pd.DataFrame:
    """Clean employees DataFrame."""
    df = df.copy()
    df.drop_duplicates(subset=["employee_id"], inplace=True)
    df["salary"] = df["salary"].clip(lower=10000, upper=500000)
    df["email"] = df["email"].str.lower().str.strip()
    df["join_date"] = pd.to_datetime(df["join_date"], errors="coerce")
    df.dropna(subset=["employee_id", "name"], inplace=True)
    log.info(f"Employees cleaned: {len(df)} rows")
    return df


def merge_datasets(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Merge attendance, employees, and departments into one enriched DataFrame."""
    required = {"attendance", "employees", "departments"}
    missing = required - set(frames.keys())
    if missing:
        log.error(f"Missing datasets: {missing}. Run generate_data.py first.")
        return pd.DataFrame()

    attendance = frames.get("attendance_dirty", frames["attendance"])
    employees = frames["employees"]
    departments = frames["departments"]

    # Clean first
    attendance = clean_attendance(attendance)
    employees = clean_employees(employees)

    # Merge attendance ← employees
    merged = attendance.merge(
        employees[["employee_id", "name", "designation", "salary", "join_date", "is_active"]],
        on="employee_id",
        how="left",
        suffixes=("_att", "_emp"),
    )

    # Merge in departments
    merged = merged.merge(
        departments.rename(columns={"department_name": "department"})[["department", "head"]],
        on="department",
        how="left",
    )

    # Derived columns
    merged["is_weekend"] = pd.to_datetime(merged["date"]).dt.weekday >= 5
    merged["month"] = pd.to_datetime(merged["date"]).dt.month
    merged["year"] = pd.to_datetime(merged["date"]).dt.year
    merged["week"] = pd.to_datetime(merged["date"]).dt.isocalendar().week.astype(int)

    log.info(f"Merged dataset: {len(merged)} rows × {len(merged.columns)} columns")
    return merged


def generate_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Generate per-employee attendance summary."""
    summary = df.groupby("employee_id").agg(
        total_days=("date", "count"),
        present_days=("status", lambda x: (x == "Present").sum()),
        absent_days=("status", lambda x: (x == "Absent").sum()),
        late_days=("status", lambda x: (x == "Late").sum()),
        wfh_days=("status", lambda x: (x == "Work-From-Home").sum()),
        total_hours=("hours_worked", "sum"),
        avg_hours=("hours_worked", "mean"),
        total_overtime=("overtime_hours", "sum"),
    ).reset_index()

    summary["attendance_pct"] = round(100 * summary["present_days"] / summary["total_days"], 2)
    return summary


def main():
    log.info("=== Task 3: CSV Reading, Cleaning & Merging ===\n")
    os.makedirs("data/processed", exist_ok=True)

    frames = read_csv_files("data/raw")
    if not frames:
        log.error("No data found! Please run: python scripts/generate_data.py")
        return

    merged = merge_datasets(frames)
    if merged.empty:
        return

    merged.to_csv("data/processed/attendance_enriched.csv", index=False)
    log.info("  Saved → data/processed/attendance_enriched.csv")

    summary = generate_summary(merged)
    summary.to_csv("data/processed/employee_summary.csv", index=False)
    log.info(f"  Saved → data/processed/employee_summary.csv ({len(summary)} employees)")

    log.info("\nSample attendance summary:")
    print(summary.head(5).to_string(index=False))

    log.info("\n✅ Task 3 Complete!")


if __name__ == "__main__":
    main()
