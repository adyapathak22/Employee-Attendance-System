"""
tests/test_pipeline.py
Unit tests for the Employee Attendance System data pipeline
Run with: pytest tests/ -v
"""
import pytest
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.transformation.modules.transformers import Normalizer, Aggregator, Validator
from src.ingestion.csv_merger import clean_attendance, clean_employees


# ── Fixtures ──────────────────────────────────────────────────
@pytest.fixture
def sample_attendance():
    return pd.DataFrame({
        "attendance_id": range(1, 11),
        "employee_id":   [f"EMP{i:04d}" for i in range(1, 11)],
        "date":          pd.date_range("2024-01-01", periods=10),
        "day_of_week":   ["Monday"] * 10,
        "shift":         ["Morning"] * 10,
        "status":        ["Present", "Absent", "Late", None, "Present",
                          "Work-From-Home", "Half-Day", "Present", "On-Leave", "Present"],
        "hours_worked":  [8.0, 0.0, 7.0, None, 8.5, 8.0, 4.5, 9.0, 0.0, 8.0],
        "overtime_hours":[0.0, 0.0, 0.0, 0.0, 0.5, 0.0, 0.0, 1.0, 0.0, 0.0],
        "department":    ["Engineering"] * 10,
        "location":      ["Lucknow"] * 10,
        "notes":         [None] * 10,
    })


@pytest.fixture
def sample_employees():
    return pd.DataFrame({
        "employee_id":  [f"EMP{i:04d}" for i in range(1, 6)],
        "name":         ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email":        ["a@test.com", "b@test.com", "c@test.com", "d@test.com", "e@test.com"],
        "department":   ["Engineering", "HR", "Finance", "Engineering", "Sales"],
        "designation":  ["Analyst"] * 5,
        "salary":       [60000, 55000, 70000, 65000, 50000],
        "is_active":    [True] * 5,
        "join_date":    ["2022-01-01"] * 5,
        "location":     ["Lucknow"] * 5,
    })


# ── Normalizer Tests ──────────────────────────────────────────
class TestNormalizer:
    def test_min_max_range(self):
        s = pd.Series([0.0, 5.0, 10.0])
        result = Normalizer.min_max(s)
        assert result.min() == pytest.approx(0.0)
        assert result.max() == pytest.approx(1.0)

    def test_min_max_constant(self):
        s = pd.Series([5.0, 5.0, 5.0])
        result = Normalizer.min_max(s)
        assert (result == 0).all()

    def test_z_score_mean(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        result = Normalizer.z_score(s)
        assert result.mean() == pytest.approx(0.0, abs=1e-10)
        assert result.std() == pytest.approx(1.0, abs=1e-10)

    def test_log_transform_non_negative(self):
        s = pd.Series([0.0, 1.0, 8.0, 100.0])
        result = Normalizer.log_transform(s)
        assert (result >= 0).all()

    def test_normalize_columns(self):
        df = pd.DataFrame({"hours": [0, 4, 8, 12], "overtime": [0, 0.5, 1, 2]})
        result = Normalizer.normalize_columns(df, ["hours", "overtime"])
        assert "hours_norm" in result.columns
        assert "overtime_norm" in result.columns


# ── Aggregator Tests ──────────────────────────────────────────
class TestAggregator:
    def test_daily_summary_columns(self, sample_attendance):
        result = Aggregator.daily_summary(sample_attendance)
        assert "date" in result.columns
        assert "present_count" in result.columns
        assert "absent_count" in result.columns

    def test_department_summary(self, sample_attendance):
        result = Aggregator.department_summary(sample_attendance)
        assert len(result) == 1  # All rows are "Engineering"
        assert "headcount" in result.columns

    def test_rolling_avg(self, sample_attendance):
        result = Aggregator.rolling_avg(sample_attendance, "hours_worked", window=3)
        assert "hours_worked_rolling_3d" in result.columns


# ── Validator Tests ───────────────────────────────────────────
class TestValidator:
    def test_check_nulls_passes(self, sample_attendance):
        v = Validator()
        # attendance_id has no nulls
        passed = v.check_nulls(sample_attendance.dropna(subset=["employee_id"]), ["employee_id"])
        assert passed is True

    def test_check_nulls_fails(self, sample_attendance):
        v = Validator()
        passed = v.check_nulls(sample_attendance, ["status"])  # has None
        assert passed is False

    def test_check_range_passes(self, sample_attendance):
        v = Validator()
        df = sample_attendance.dropna(subset=["hours_worked"])
        passed = v.check_range(df, "hours_worked", 0, 14)
        assert passed is True

    def test_check_range_fails(self):
        v = Validator()
        df = pd.DataFrame({"hours": [8, 20, -1]})
        passed = v.check_range(df, "hours", 0, 14)
        assert passed is False

    def test_check_allowed_values(self, sample_attendance):
        v = Validator()
        valid = {"Present", "Absent", "Late", "Half-Day", "Work-From-Home", "On-Leave", None}
        df = sample_attendance.dropna(subset=["status"])
        passed = v.check_allowed_values(df, "status",
                    ["Present", "Absent", "Late", "Half-Day", "Work-From-Home", "On-Leave"])
        # all valid statuses present
        assert isinstance(passed, bool)

    def test_report_format(self, sample_attendance):
        v = Validator()
        v.check_nulls(sample_attendance, ["employee_id"])
        report = v.report()
        assert isinstance(report, pd.DataFrame)
        assert "check" in report.columns
        assert "passed" in report.columns


# ── Cleaning Tests ────────────────────────────────────────────
class TestCleaning:
    def test_clean_attendance_fills_nulls(self, sample_attendance):
        result = clean_attendance(sample_attendance)
        assert result["status"].isnull().sum() == 0
        assert result["hours_worked"].isnull().sum() == 0

    def test_clean_attendance_clips_hours(self):
        df = pd.DataFrame({
            "attendance_id": [1, 2],
            "employee_id": ["EMP0001", "EMP0002"],
            "date": ["2024-01-01", "2024-01-02"],
            "status": ["Present", "Present"],
            "hours_worked": [20.0, -1.0],
            "overtime_hours": [5.0, 0.0],
            "day_of_week": ["Monday", "Tuesday"],
            "shift": ["Morning", "Morning"],
            "department": ["HR", "HR"],
            "location": ["LKO", "LKO"],
            "notes": [None, None],
        })
        result = clean_attendance(df)
        assert (result["hours_worked"] <= 14).all()
        assert (result["hours_worked"] >= 0).all()

    def test_clean_employees_deduplication(self, sample_employees):
        duped = pd.concat([sample_employees, sample_employees.iloc[:2]])
        result = clean_employees(duped)
        assert result["employee_id"].duplicated().sum() == 0

    def test_clean_employees_email_lowercase(self, sample_employees):
        emp = sample_employees.copy()
        emp["email"] = emp["email"].str.upper()
        result = clean_employees(emp)
        assert (result["email"] == result["email"].str.lower()).all()


# ── Integration Test ──────────────────────────────────────────
class TestIntegration:
    def test_full_normalize_and_aggregate(self, sample_attendance):
        df = clean_attendance(sample_attendance)
        df = Normalizer.normalize_columns(df, ["hours_worked"], "min_max")
        assert "hours_worked_norm" in df.columns
        summary = Aggregator.department_summary(df)
        assert len(summary) > 0

    def test_validator_summary(self, sample_attendance):
        v = Validator()
        v.check_nulls(sample_attendance, ["employee_id", "date"])
        v.check_range(sample_attendance.fillna(0), "hours_worked", 0, 14)
        summary = v.summary()
        assert summary["total_checks"] == 3
        assert "pass_rate" in summary
