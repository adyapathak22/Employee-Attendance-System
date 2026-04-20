"""
src/transformation/modules/transformers.py
TASK 4 — Advanced Python: Reusable modules for normalization, aggregation, validation
"""
import pandas as pd
import numpy as np
from typing import Callable
import logging

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Normalization Functions
# ──────────────────────────────────────────────────────────────
class Normalizer:
    """Statistical normalization methods for numerical attendance features."""

    @staticmethod
    def min_max(series: pd.Series) -> pd.Series:
        """Scale values to [0, 1]."""
        mn, mx = series.min(), series.max()
        if mx == mn:
            return pd.Series(np.zeros(len(series)), index=series.index)
        return (series - mn) / (mx - mn)

    @staticmethod
    def z_score(series: pd.Series) -> pd.Series:
        """Standardize: mean=0, std=1."""
        mean, std = series.mean(), series.std()
        if std == 0:
            return pd.Series(np.zeros(len(series)), index=series.index)
        return (series - mean) / std

    @staticmethod
    def robust_scale(series: pd.Series) -> pd.Series:
        """Scale using IQR (resistant to outliers)."""
        q1, q3 = series.quantile(0.25), series.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return pd.Series(np.zeros(len(series)), index=series.index)
        return (series - series.median()) / iqr

    @staticmethod
    def log_transform(series: pd.Series) -> pd.Series:
        """Log1p transform for skewed distributions."""
        return np.log1p(series.clip(lower=0))

    @classmethod
    def normalize_columns(cls, df: pd.DataFrame, columns: list[str], method: str = "min_max") -> pd.DataFrame:
        """Normalize multiple columns at once."""
        df = df.copy()
        fn: Callable = getattr(cls, method)
        for col in columns:
            if col in df.columns:
                df[f"{col}_norm"] = fn(df[col])
                log.info(f"  Normalized '{col}' using {method}")
        return df


# ──────────────────────────────────────────────────────────────
# Aggregation Functions
# ──────────────────────────────────────────────────────────────
class Aggregator:
    """Reusable attendance aggregation logic."""

    @staticmethod
    def daily_summary(df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate attendance metrics per day."""
        return df.groupby("date").agg(
            total_employees=("employee_id", "nunique"),
            present_count=("status", lambda x: (x == "Present").sum()),
            absent_count=("status", lambda x: (x == "Absent").sum()),
            late_count=("status", lambda x: (x == "Late").sum()),
            wfh_count=("status", lambda x: (x == "Work-From-Home").sum()),
            avg_hours=("hours_worked", "mean"),
            total_overtime=("overtime_hours", "sum"),
        ).reset_index()

    @staticmethod
    def monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
        """Monthly aggregation per employee."""
        df = df.copy()
        df["year_month"] = pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
        return df.groupby(["employee_id", "year_month"]).agg(
            working_days=("date", "count"),
            present=("status", lambda x: (x == "Present").sum()),
            absent=("status", lambda x: (x == "Absent").sum()),
            hours_worked=("hours_worked", "sum"),
            overtime=("overtime_hours", "sum"),
        ).reset_index()

    @staticmethod
    def department_summary(df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate by department."""
        return df.groupby("department").agg(
            headcount=("employee_id", "nunique"),
            avg_attendance_pct=("status", lambda x: round(100 * (x == "Present").sum() / len(x), 2)),
            total_hours=("hours_worked", "sum"),
            avg_overtime=("overtime_hours", "mean"),
        ).reset_index()

    @staticmethod
    def rolling_avg(df: pd.DataFrame, col: str, window: int = 7) -> pd.DataFrame:
        """Calculate rolling average over a time window."""
        df = df.sort_values("date").copy()
        df[f"{col}_rolling_{window}d"] = df[col].rolling(window=window, min_periods=1).mean()
        return df


# ──────────────────────────────────────────────────────────────
# Validation Functions
# ──────────────────────────────────────────────────────────────
class Validator:
    """Rule-based data validation with detailed reporting."""

    def __init__(self):
        self.results = []

    def check_nulls(self, df: pd.DataFrame, critical_cols: list[str]) -> bool:
        """Ensure critical columns have no nulls."""
        for col in critical_cols:
            null_count = df[col].isnull().sum()
            passed = null_count == 0
            self.results.append({
                "check": f"null_check:{col}",
                "passed": passed,
                "detail": f"{null_count} nulls found" if not passed else "✓",
            })
        return all(r["passed"] for r in self.results if r["check"].startswith("null_check"))

    def check_range(self, df: pd.DataFrame, col: str, min_val: float, max_val: float) -> bool:
        """Validate values fall within expected range."""
        violations = ((df[col] < min_val) | (df[col] > max_val)).sum()
        passed = violations == 0
        self.results.append({
            "check": f"range_check:{col}[{min_val},{max_val}]",
            "passed": passed,
            "detail": f"{violations} violations" if not passed else "✓",
        })
        return passed

    def check_allowed_values(self, df: pd.DataFrame, col: str, allowed: list) -> bool:
        """Ensure column only contains allowed categorical values."""
        invalid = ~df[col].isin(allowed)
        violations = invalid.sum()
        passed = violations == 0
        self.results.append({
            "check": f"category_check:{col}",
            "passed": passed,
            "detail": f"{violations} invalid values: {df.loc[invalid, col].unique().tolist()[:5]}" if not passed else "✓",
        })
        return passed

    def check_referential_integrity(self, df: pd.DataFrame, col: str, ref_df: pd.DataFrame, ref_col: str) -> bool:
        """Check foreign key integrity."""
        orphans = ~df[col].isin(ref_df[ref_col])
        violations = orphans.sum()
        passed = violations == 0
        self.results.append({
            "check": f"fk_check:{col}→{ref_col}",
            "passed": passed,
            "detail": f"{violations} orphan records" if not passed else "✓",
        })
        return passed

    def report(self) -> pd.DataFrame:
        return pd.DataFrame(self.results)

    def summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        return {
            "total_checks": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": f"{100 * passed / total:.1f}%" if total else "N/A",
        }
