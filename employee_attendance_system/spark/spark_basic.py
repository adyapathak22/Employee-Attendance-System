import os
import time
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# TASK 14: Pandas Basics — Load and process attendance CSV
# ──────────────────────────────────────────────────────────────
def task14_pandas_basics():
    log.info("\n═══ Task 14: Pandas Basics ═══")

    att_path = "data/raw/attendance.csv"
    if not Path(att_path).exists():
        log.error("Run generate_data.py first!")
        return None

    df = pd.read_csv(att_path)

    log.info(f"Loaded: {df.shape[0]} rows × {df.shape[1]} cols")
    print(df.head())

    log.info("Status distribution:")
    print(df["status"].value_counts())

    log.info("Top departments by avg hours:")
    print(df.groupby("department")["hours_worked"].mean().sort_values(ascending=False))

    return df


# ──────────────────────────────────────────────────────────────
# TASK 15: Pandas Transformations
# ──────────────────────────────────────────────────────────────
def task15_pandas_transform(df):
    log.info("\n═══ Task 15: Pandas Transformations ═══")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["day_num"] = df["date"].dt.dayofweek

    df["hours_worked"] = pd.to_numeric(df["hours_worked"], errors="coerce").fillna(0)
    df["overtime_hours"] = pd.to_numeric(df["overtime_hours"], errors="coerce").fillna(0)

    df["status"] = df["status"].fillna("Unknown")
    df["is_present"] = df["status"].apply(lambda x: 1 if x == "Present" else 0)

    df["productivity_score"] = (df["hours_worked"] / 8).clip(upper=1.5).round(3)

    log.info("Productivity stats:")
    print(df["productivity_score"].describe())

    log.info("WFH trend by month:")
    print(
        df[df["status"] == "Work-From-Home"]
        .groupby(["year", "month"])
        .size()
        .reset_index(name="count")
    )

    return df


# ──────────────────────────────────────────────────────────────
# TASK 16: SQL-like Analysis using Pandas
# ──────────────────────────────────────────────────────────────
def task16_pandas_sql(df):
    log.info("\n═══ Task 16: Pandas SQL-like Analysis ═══")

    # Monthly attendance rate
    result = (
        df.groupby(["department", "year", "month"])
        .agg(total=("employee_id", "count"),
             present=("is_present", "sum"))
        .reset_index()
    )
    result["attendance_pct"] = (100 * result["present"] / result["total"]).round(2)

    print("\nMonthly attendance rate:")
    print(result.head(20))

    # Top overtime employees
    top_ot = (
        df[df["overtime_hours"] > 0]
        .groupby("employee_id")
        .agg(total_ot=("overtime_hours", "sum"),
             records=("employee_id", "count"))
        .sort_values("total_ot", ascending=False)
        .head(10)
    )

    print("\nTop 10 overtime employees:")
    print(top_ot)

    # Absenteeism pattern
    absent = (
        df[df["status"] == "Absent"]
        .groupby("day_num")
        .size()
        .reset_index(name="absent_count")
    )

    absent["pct"] = (100 * absent["absent_count"] / absent["absent_count"].sum()).round(2)

    print("\nAbsenteeism pattern:")
    print(absent)

    # Late arrivals
    late = (
        df.groupby("department")
        .apply(lambda x: pd.Series({
            "late_count": (x["status"] == "Late").sum(),
            "avg_hours": round(x["hours_worked"].mean(), 2)
        }))
        .sort_values("late_count", ascending=False)
    )

    print("\nLate arrivals by department:")
    print(late)


# ──────────────────────────────────────────────────────────────
# TASK 17: Optimization-like operations
# ──────────────────────────────────────────────────────────────
def task17_pandas_advanced(df):
    log.info("\n═══ Task 17: Pandas Advanced ═══")

    os.makedirs("data/processed", exist_ok=True)

    # Save as parquet (fast)
    t = time.time()
    df.to_parquet("data/processed/attendance.parquet", index=False)
    log.info(f"Saved parquet in {round(time.time()-t, 2)}s")

    # Reload
    t = time.time()
    df2 = pd.read_parquet("data/processed/attendance.parquet")
    log.info(f"Reloaded parquet in {round(time.time()-t, 2)}s")

    # Top performers per department
    ranked = df.sort_values(["department", "hours_worked"], ascending=[True, False])
    top3 = ranked.groupby("department").head(3)

    print("\nTop 3 employees per department:")
    print(top3[["employee_id", "department", "hours_worked"]])


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
def main():
    df = task14_pandas_basics()
    if df is None:
        return

    df = task15_pandas_transform(df)
    task16_pandas_sql(df)
    task17_pandas_advanced(df)


if __name__ == "__main__":
    main()