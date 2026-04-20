"""
airflow/dags/etl_dag.py
TASK 22 — Airflow Basics: DAG for ETL pipeline
TASK 23 — Airflow Advanced: Scheduling, monitoring, retry mechanisms
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import logging

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Default arguments — applied to all tasks unless overridden
# ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email": ["dataops@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,                            # Task 23: retry mechanism
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,       # Task 23: exponential backoff
    "max_retry_delay": timedelta(hours=1),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),               # Task 23: SLA alerting
}


# ──────────────────────────────────────────────────────────────
# Python callable functions for each task
# ──────────────────────────────────────────────────────────────
def extract_attendance(**context):
    """Extract raw attendance data from source."""
    import pandas as pd
    import os
    log.info(f"[EXTRACT] Execution date: {context['ds']}")
    raw_path = "data/raw/attendance.csv"
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Source not found: {raw_path}")
    df = pd.read_csv(raw_path, low_memory=False)
    log.info(f"[EXTRACT] Loaded {len(df):,} rows")
    # Push to XCom
    context["ti"].xcom_push(key="row_count", value=len(df))
    context["ti"].xcom_push(key="source_path", value=raw_path)
    return len(df)


def validate_data(**context):
    """Validate extracted data quality before transform."""
    import pandas as pd
    source = context["ti"].xcom_pull(key="source_path", task_ids="extract")
    row_count = context["ti"].xcom_pull(key="row_count", task_ids="extract")

    if not source or row_count == 0:
        raise ValueError("No data extracted!")

    df = pd.read_csv(source, low_memory=False)
    null_rate = df.isnull().sum().sum() / (len(df) * len(df.columns))
    log.info(f"[VALIDATE] Null rate: {null_rate:.2%}")

    if null_rate > 0.3:
        raise ValueError(f"Data quality failed: null rate {null_rate:.2%} > 30%")

    context["ti"].xcom_push(key="null_rate", value=round(null_rate, 4))
    log.info("[VALIDATE] ✓ Data quality check passed")


def branch_check_data_quality(**context):
    """Branch: route to transform if quality OK, else alert."""
    null_rate = context["ti"].xcom_pull(key="null_rate", task_ids="validate")
    if null_rate is not None and null_rate < 0.1:
        return "transform"
    return "alert_quality_issue"


def transform_data(**context):
    """Transform and clean attendance data."""
    import pandas as pd
    import os
    log.info("[TRANSFORM] Running transformation pipeline...")
    os.makedirs("data/processed", exist_ok=True)

    df = pd.read_csv("data/raw/attendance.csv", low_memory=False)
    df["status"].fillna("Unknown", inplace=True)
    df["hours_worked"].fillna(0, inplace=True)
    df["hours_worked"] = df["hours_worked"].clip(0, 14)
    df["overtime_hours"] = df["overtime_hours"].clip(0, 4)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.dropna(subset=["date", "employee_id"], inplace=True)
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["is_present"] = (df["status"] == "Present").astype(int)

    out = f"data/processed/attendance_{context['ds_nodash']}.parquet"
    df.to_parquet(out, index=False)
    log.info(f"[TRANSFORM] ✓ Saved {len(df):,} rows → {out}")
    context["ti"].xcom_push(key="output_path", value=out)


def load_to_warehouse(**context):
    """Load transformed data to data warehouse (SQLite)."""
    import pandas as pd
    import sqlite3
    output_path = context["ti"].xcom_pull(key="output_path", task_ids="transform")
    if not output_path:
        raise ValueError("No transformed file found")

    df = pd.read_parquet(output_path)
    with sqlite3.connect("data/attendance.db") as conn:
        df.to_sql("dw_attendance_daily", conn, if_exists="append", index=False)
    log.info(f"[LOAD] ✓ {len(df):,} rows → DW table: dw_attendance_daily")


def generate_report(**context):
    """Generate attendance KPI report."""
    import pandas as pd
    import sqlite3
    import json
    import os
    log.info("[REPORT] Generating KPI summary...")
    try:
        with sqlite3.connect("data/attendance.db") as conn:
            df = pd.read_sql("SELECT * FROM dw_attendance_daily", conn)
        kpis = {
            "date": context["ds"],
            "total_records": len(df),
            "present_pct": round(100 * df["is_present"].mean(), 2),
            "avg_hours": round(df["hours_worked"].mean(), 2),
            "total_overtime": round(df["overtime_hours"].sum(), 2),
        }
        os.makedirs("data/reports", exist_ok=True)
        with open(f"data/reports/kpi_{context['ds_nodash']}.json", "w") as f:
            json.dump(kpis, f, indent=2)
        log.info(f"[REPORT] ✓ KPIs: {kpis}")
    except Exception as e:
        log.warning(f"[REPORT] Report generation warning: {e}")


def alert_quality_issue(**context):
    """Alert on data quality failure."""
    log.warning("[ALERT] Data quality issue detected! Notifying team...")
    # In production: send Slack/email/PagerDuty alert
    null_rate = context["ti"].xcom_pull(key="null_rate", task_ids="validate") or "unknown"
    log.warning(f"[ALERT] Null rate: {null_rate}")


def cleanup_temp_files(**context):
    """Clean up temporary processing files."""
    import glob
    import os
    files = glob.glob("data/processed/*.parquet")
    # Keep last 7 days worth of files
    cutoff = datetime.now() - timedelta(days=7)
    removed = 0
    for f in files:
        if os.path.getmtime(f) < cutoff.timestamp():
            os.remove(f)
            removed += 1
    log.info(f"[CLEANUP] Removed {removed} old temp files")


# ──────────────────────────────────────────────────────────────
# TASK 22: Basic DAG definition
# ──────────────────────────────────────────────────────────────
with DAG(
    dag_id="attendance_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="Employee Attendance ETL Pipeline",
    schedule_interval="0 2 * * *",   # Task 23: Daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,               # Task 23: Prevent overlapping runs
    tags=["attendance", "etl", "daily"],
    doc_md="""
    ## Attendance ETL Pipeline
    Ingests raw attendance data, validates quality, transforms,
    loads to warehouse, and generates daily KPI reports.

    **Schedule:** Daily at 02:00 UTC
    **SLA:** Must complete within 3 hours
    **Owner:** Data Engineering Team
    """,
) as dag:

    # Start marker
    start = EmptyOperator(task_id="start")

    # Task 22: Core ETL tasks
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_attendance,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data,
    )

    # Task 23: Branching based on data quality
    branch = BranchPythonOperator(
        task_id="branch_quality_check",
        python_callable=branch_check_data_quality,
    )

    alert = PythonOperator(
        task_id="alert_quality_issue",
        python_callable=alert_quality_issue,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        pool="heavy_tasks",          # Task 23: task pool for resource control
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_to_warehouse,
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # Task 23: custom trigger rule
    )

    # Task 23: Bash task for dbt-style data check
    dq_check = BashOperator(
        task_id="run_dq_checks",
        bash_command="python src/validation/data_quality.py || echo 'DQ checks had warnings'",
        retries=1,
    )

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup_temp_files,
        trigger_rule=TriggerRule.ALL_DONE,    # Always run cleanup
    )

    end = EmptyOperator(task_id="end")

    # ── DAG dependency chain ──────────────────────────────────
    start >> extract >> validate >> branch
    branch >> [transform, alert]
    transform >> load >> report >> dq_check >> cleanup >> end
    alert >> end
