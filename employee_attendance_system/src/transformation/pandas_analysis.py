"""
src/transformation/pandas_analysis.py
TASK 5 — Pandas + NumPy: Large-scale analysis, memory optimization, performance comparison
"""
import pandas as pd
import numpy as np
import time
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)


def memory_usage_mb(df: pd.DataFrame) -> float:
    return round(df.memory_usage(deep=True).sum() / 1024 ** 2, 3)


# ──────────────────────────────────────────────────────────────
# Task 5a: Load and profile the dataset
# ──────────────────────────────────────────────────────────────
def load_and_profile(path: str) -> pd.DataFrame:
    log.info(f"Loading {path}...")
    start = time.time()
    df = pd.read_csv(path, low_memory=False)
    load_time = round(time.time() - start, 3)
    mem = memory_usage_mb(df)
    log.info(f"  Loaded {len(df):,} rows × {len(df.columns)} cols in {load_time}s | Memory: {mem}MB")
    log.info(f"  Dtypes:\n{df.dtypes.value_counts().to_string()}")
    log.info(f"  Nulls:\n{df.isnull().sum()[df.isnull().sum() > 0].to_string()}")
    return df


# ──────────────────────────────────────────────────────────────
# Task 5b: Optimize memory usage
# ──────────────────────────────────────────────────────────────
def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast numerics and use categorical for low-cardinality strings."""
    log.info("Optimizing dtypes...")
    before = memory_usage_mb(df)
    df = df.copy()

    # Downcast integers
    for col in df.select_dtypes("int64").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")

    # Downcast floats
    for col in df.select_dtypes("float64").columns:
        df[col] = pd.to_numeric(df[col], downcast="float")

    # Convert low-cardinality strings to category
    for col in df.select_dtypes("object").columns:
        n_unique = df[col].nunique()
        if n_unique / len(df) < 0.1:  # <10% unique = good candidate
            df[col] = df[col].astype("category")

    after = memory_usage_mb(df)
    reduction = round(100 * (before - after) / before, 1)
    log.info(f"  Memory: {before}MB → {after}MB ({reduction}% reduction)")
    return df


# ──────────────────────────────────────────────────────────────
# Task 5c: Performance comparison — loop vs vectorized vs NumPy
# ──────────────────────────────────────────────────────────────
def performance_comparison(df: pd.DataFrame):
    hours = df["hours_worked"].dropna().values
    log.info(f"\nPerformance comparison on {len(hours):,} values:")

    # Method 1: Python loop
    start = time.perf_counter()
    total = 0
    for h in hours:
        total += h
    loop_time = time.perf_counter() - start
    log.info(f"  Python loop:       {loop_time:.5f}s → sum={total:.2f}")

    # Method 2: Pandas .sum()
    start = time.perf_counter()
    pd_sum = pd.Series(hours).sum()
    pd_time = time.perf_counter() - start
    log.info(f"  Pandas .sum():     {pd_time:.5f}s → sum={pd_sum:.2f}")

    # Method 3: NumPy
    start = time.perf_counter()
    np_sum = np.sum(hours)
    np_time = time.perf_counter() - start
    log.info(f"  NumPy np.sum():    {np_time:.5f}s → sum={np_sum:.2f}")

    speedup_pd = round(loop_time / pd_time, 1) if pd_time > 0 else "∞"
    speedup_np = round(loop_time / np_time, 1) if np_time > 0 else "∞"
    log.info(f"  Pandas is {speedup_pd}x faster | NumPy is {speedup_np}x faster than Python loop")


# ──────────────────────────────────────────────────────────────
# Task 5d: Advanced analysis with NumPy
# ──────────────────────────────────────────────────────────────
def numpy_analysis(df: pd.DataFrame):
    log.info("\n── NumPy Statistical Analysis ──────────────────────────")
    hours = df["hours_worked"].dropna().to_numpy()

    print(f"  Count:        {len(hours):,}")
    print(f"  Mean:         {np.mean(hours):.3f} hrs")
    print(f"  Median:       {np.median(hours):.3f} hrs")
    print(f"  Std Dev:      {np.std(hours):.3f}")
    print(f"  Variance:     {np.var(hours):.3f}")
    print(f"  Min/Max:      {np.min(hours):.1f} / {np.max(hours):.1f}")
    print(f"  Percentiles (25/50/75/90/99):")
    for p in [25, 50, 75, 90, 99]:
        print(f"    P{p}: {np.percentile(hours, p):.2f}")

    # Detect outliers with IQR
    q1, q3 = np.percentile(hours, [25, 75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outliers = hours[(hours < lower) | (hours > upper)]
    print(f"\n  Outliers (IQR method): {len(outliers)} records")
    print(f"  Valid range: [{lower:.2f}, {upper:.2f}]")

    # Correlation matrix
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr()
        print(f"\n  Correlation matrix (numeric cols):\n{corr.round(3)}")


# ──────────────────────────────────────────────────────────────
# Task 5e: Chunked reading for very large files (1M+ rows)
# ──────────────────────────────────────────────────────────────
def chunked_aggregate(path: str, chunk_size: int = 50_000) -> pd.DataFrame:
    """Aggregate data in chunks — scalable to 1M+ rows without full memory load."""
    log.info(f"\nChunked aggregation (chunk_size={chunk_size:,})...")
    dept_hours = {}
    total_rows = 0

    try:
        for chunk in pd.read_csv(path, chunksize=chunk_size, low_memory=False):
            total_rows += len(chunk)
            for dept, grp in chunk.groupby("department"):
                if dept not in dept_hours:
                    dept_hours[dept] = {"total_hours": 0, "count": 0}
                dept_hours[dept]["total_hours"] += grp["hours_worked"].sum()
                dept_hours[dept]["count"] += len(grp)

        result = pd.DataFrame([
            {"department": d, "avg_hours": v["total_hours"] / v["count"], "total_rows": v["count"]}
            for d, v in dept_hours.items()
        ]).sort_values("avg_hours", ascending=False)

        log.info(f"  Processed {total_rows:,} rows in chunks")
        log.info(f"  Dept avg hours:\n{result.to_string(index=False)}")
        return result
    except FileNotFoundError:
        log.warning("  attendance.csv not found. Run generate_data.py first.")
        return pd.DataFrame()


def main():
    log.info("=== Task 5: Pandas + NumPy Large-Scale Analysis ===\n")

    att_path = "data/raw/attendance.csv"
    if not Path(att_path).exists():
        log.error("Run: python scripts/generate_data.py first!")
        return

    df = load_and_profile(att_path)
    df_opt = optimize_dtypes(df)
    performance_comparison(df_opt)
    numpy_analysis(df_opt)
    chunked_aggregate(att_path)

    # Save optimized data
    os.makedirs("data/processed", exist_ok=True)
    df_opt.to_parquet("data/processed/attendance_optimized.parquet", index=False)
    log.info("\n  Saved → data/processed/attendance_optimized.parquet")
    log.info("✅ Task 5 Complete!")


if __name__ == "__main__":
    main()
