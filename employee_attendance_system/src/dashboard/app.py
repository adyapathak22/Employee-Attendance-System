"""
src/dashboard/app.py
TASK 30 — Final Project: End-to-end pipeline dashboard
Ingestion → Processing → Storage → Orchestration → Dashboard
Run with: streamlit run src/dashboard/app.py
"""
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# ──────────────────────────────────────────────────────────────
# Page configuration
# ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Employee Attendance System",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e, #16213e);
        border-radius: 12px; padding: 20px; margin: 8px 0;
        border-left: 4px solid #0f3460;
    }
    .kpi-value { font-size: 2.2rem; font-weight: 800; color: #e94560; }
    .kpi-label { color: #a0aec0; font-size: 0.85rem; text-transform: uppercase; }
    .section-header {
        font-size: 1.4rem; font-weight: 700;
        border-bottom: 2px solid #e94560; padding-bottom: 6px; margin: 16px 0;
    }
    .pipeline-step {
        background: #0f3460; border-radius: 8px; padding: 12px;
        margin: 4px 0; color: white; font-size: 0.9rem;
    }
    .status-ok  { color: #48bb78; font-weight: bold; }
    .status-err { color: #fc8181; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
# Data loading
# ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    db_path = "data/attendance.db"
    csv_att = "data/raw/attendance.csv"
    csv_emp = "data/raw/employees.csv"

    if Path(db_path).exists():
        try:
            with sqlite3.connect(db_path) as conn:
                att = pd.read_sql("SELECT * FROM attendance", conn)
                emp = pd.read_sql("SELECT * FROM employees", conn)
                return att, emp
        except Exception:
            pass

    if Path(csv_att).exists() and Path(csv_emp).exists():
        att = pd.read_csv(csv_att, low_memory=False)
        emp = pd.read_csv(csv_emp, low_memory=False)
        return att, emp

    return pd.DataFrame(), pd.DataFrame()


def preprocess(att: pd.DataFrame, emp: pd.DataFrame) -> pd.DataFrame:
    if att.empty:
        return pd.DataFrame()
    df = att.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["status"].fillna("Unknown", inplace=True)
    df["hours_worked"].fillna(0, inplace=True)
    df["is_present"] = (df["status"] == "Present").astype(int)
    df["month"] = df["date"].dt.to_period("M").astype(str)
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["year"] = df["date"].dt.year
    return df


# ──────────────────────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/attendance-mark.png", width=64)
    st.title("Attendance System")
    st.caption("Advanced Data Engineering Project")

    st.markdown("---")
    page = st.radio("Navigate", [
        "📊 Dashboard",
        "🔄 Run Pipeline",
        "📋 Data Explorer",
        "✅ Data Quality",
        "🏗️ Architecture",
        "📚 Task Reference",
    ])

    st.markdown("---")
    st.markdown("**Pipeline Status**")
    data_exists = Path("data/raw/attendance.csv").exists()
    db_exists   = Path("data/attendance.db").exists()

    st.markdown(f"Raw Data: {'<span class=status-ok>✓ Ready</span>' if data_exists else '<span class=status-err>✗ Missing</span>'}", unsafe_allow_html=True)
    st.markdown(f"Database: {'<span class=status-ok>✓ Ready</span>' if db_exists else '<span class=status-err>✗ Missing</span>'}", unsafe_allow_html=True)
    dq_report = Path("data/reports/dq_report.json").exists()
    st.markdown(f"DQ Report: {'<span class=status-ok>✓ Done</span>' if dq_report else '<span class=status-err>✗ Pending</span>'}", unsafe_allow_html=True)


# ──────────────────────────────────────────────────────────────
# Load and prep data
# ──────────────────────────────────────────────────────────────
att_df, emp_df = load_data()
df = preprocess(att_df, emp_df)

if df.empty:
    st.warning("⚠️ No data found! Please run the pipeline first:")
    st.code("python scripts/generate_data.py\npython src/ingestion/batch_ingestor.py")
    st.stop()

# ──────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────
if page == "📊 Dashboard":
    try:
        import plotly.express as px
        import plotly.graph_objects as go
        PLOTLY = True
    except ImportError:
        PLOTLY = False
        st.warning("Install plotly for charts: pip install plotly")

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        depts = ["All"] + sorted(df["department"].dropna().unique().tolist())
        dept_filter = st.selectbox("Department", depts)
    with col_f2:
        months = sorted(df["month"].dropna().unique().tolist())
        month_filter = st.selectbox("Month", ["All"] + months[-6:])
    with col_f3:
        statuses = ["All"] + df["status"].dropna().unique().tolist()
        status_filter = st.selectbox("Status", statuses)

    filtered = df.copy()
    if dept_filter != "All":
        filtered = filtered[filtered["department"] == dept_filter]
    if month_filter != "All":
        filtered = filtered[filtered["month"] == month_filter]
    if status_filter != "All":
        filtered = filtered[filtered["status"] == status_filter]

    # ── KPI Row ──────────────────────────────────────────────
    st.markdown('<div class="section-header">📊 Key Performance Indicators</div>', unsafe_allow_html=True)
    k1, k2, k3, k4, k5 = st.columns(5)
    total_emp  = df["employee_id"].nunique()
    att_rate   = round(100 * filtered["is_present"].mean(), 1)
    avg_hours  = round(filtered["hours_worked"].mean(), 1)
    total_ot   = int(filtered["overtime_hours"].sum())
    absent_days = int((filtered["status"] == "Absent").sum())

    k1.metric("Total Employees", f"{total_emp:,}")
    k2.metric("Attendance Rate", f"{att_rate}%", delta=f"{att_rate - 85:.1f}% vs target")
    k3.metric("Avg Hours/Day", f"{avg_hours}h")
    k4.metric("Total Overtime", f"{total_ot:,}h")
    k5.metric("Absent Records", f"{absent_days:,}")

    if PLOTLY:
        # Row 1: Attendance trend + Status distribution
        r1c1, r1c2 = st.columns([2, 1])
        with r1c1:
            monthly = filtered.groupby("month").agg(
                attendance_pct=("is_present", lambda x: round(100 * x.mean(), 1))
            ).reset_index()
            fig = px.line(monthly, x="month", y="attendance_pct",
                          title="Monthly Attendance Rate (%)",
                          color_discrete_sequence=["#e94560"],
                          markers=True)
            fig.add_hline(y=85, line_dash="dash", line_color="orange",
                          annotation_text="85% target")
            fig.update_layout(template="plotly_dark", height=320)
            st.plotly_chart(fig, use_container_width=True)

        with r1c2:
            status_counts = filtered["status"].value_counts()
            fig = px.pie(values=status_counts.values, names=status_counts.index,
                         title="Attendance Status",
                         color_discrete_sequence=px.colors.sequential.Plasma_r)
            fig.update_layout(template="plotly_dark", height=320)
            st.plotly_chart(fig, use_container_width=True)

        # Row 2: Department heatmap + Overtime bar
        r2c1, r2c2 = st.columns(2)
        with r2c1:
            dept_month = filtered.groupby(["department", "month"])["is_present"].mean().reset_index()
            dept_month["attendance_pct"] = (dept_month["is_present"] * 100).round(1)
            pivot = dept_month.pivot(index="department", columns="month", values="attendance_pct")
            fig = px.imshow(pivot, title="Dept × Month Attendance Heatmap (%)",
                            color_continuous_scale="RdYlGn", aspect="auto")
            fig.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig, use_container_width=True)

        with r2c2:
            ot_dept = filtered.groupby("department")["overtime_hours"].sum().sort_values(ascending=False).reset_index()
            fig = px.bar(ot_dept, x="overtime_hours", y="department",
                         title="Total Overtime by Department",
                         orientation="h", color="overtime_hours",
                         color_continuous_scale="Reds")
            fig.update_layout(template="plotly_dark", height=350)
            st.plotly_chart(fig, use_container_width=True)

        # Row 3: Day-of-week pattern
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        dow = filtered[filtered["day_of_week"].isin(dow_order)].groupby("day_of_week")["is_present"].mean().reindex(dow_order).reset_index()
        dow["attendance_pct"] = (dow["is_present"] * 100).round(1)
        fig = px.bar(dow, x="day_of_week", y="attendance_pct",
                     title="Attendance Rate by Day of Week",
                     color="attendance_pct", color_continuous_scale="Blues")
        fig.update_layout(template="plotly_dark", height=280)
        st.plotly_chart(fig, use_container_width=True)

# ──────────────────────────────────────────────────────────────
elif page == "🔄 Run Pipeline":
    st.markdown('<div class="section-header">🔄 Pipeline Orchestration</div>', unsafe_allow_html=True)
    st.write("Run each pipeline stage in order:")

    stages = [
        ("1. Generate Sample Data",    "python scripts/generate_data.py",              "Generates CSVs in data/raw/"),
        ("2. Linux Setup",             "bash scripts/setup_linux.sh",                   "Sets up directory permissions & logs"),
        ("3. Batch Ingestion",         "python src/ingestion/batch_ingestor.py",         "Loads CSVs into SQLite DB"),
        ("4. Transformation",          "python src/transformation/pandas_analysis.py",   "Pandas/NumPy analysis"),
        ("5. ETL vs ELT",              "python src/ingestion/etl_elt_compare.py",        "Compare ETL and ELT approaches"),
        ("6. SQL Setup",               "python sql/setup_database.py",                  "Creates OLTP + Star schema"),
        ("7. Data Quality",            "python src/validation/data_quality.py",          "Runs 29 quality checks"),
        ("8. Kafka Simulation",        "python kafka/producer.py",                      "Simulates real-time events"),
    ]

    for name, cmd, desc in stages:
        with st.expander(f"{name}"):
            st.code(cmd, language="bash")
            st.caption(desc)
            if st.button(f"▶ Run", key=f"btn_{name}"):
                with st.spinner(f"Running {name}..."):
                    result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=120)
                if result.returncode == 0:
                    st.success("✓ Completed!")
                    if result.stdout:
                        st.text(result.stdout[-2000:])
                else:
                    st.error("✗ Failed")
                    st.text(result.stderr[-1000:])

# ──────────────────────────────────────────────────────────────
elif page == "📋 Data Explorer":
    st.markdown('<div class="section-header">📋 Data Explorer</div>', unsafe_allow_html=True)

    table = st.selectbox("Select table", ["attendance", "employees"])
    data = att_df if table == "attendance" else emp_df

    if not data.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Rows", f"{len(data):,}")
        col2.metric("Columns", len(data.columns))
        col3.metric("Missing Values", int(data.isnull().sum().sum()))

        st.subheader("Preview")
        st.dataframe(data.head(100), use_container_width=True)

        st.subheader("Column Statistics")
        st.dataframe(data.describe(include="all").T, use_container_width=True)

# ──────────────────────────────────────────────────────────────
elif page == "✅ Data Quality":
    st.markdown('<div class="section-header">✅ Data Quality Report</div>', unsafe_allow_html=True)
    report_path = "data/reports/dq_report.json"
    if Path(report_path).exists():
        with open(report_path) as f:
            report = json.load(f)
        summary = report["summary"]
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Checks", summary["total"])
        c2.metric("Passed", summary["passed"])
        c3.metric("Failed", summary["failed"])
        results_df = pd.DataFrame(report["results"])
        st.dataframe(results_df[["check_name", "table", "column", "passed", "severity", "expected", "actual"]],
                     use_container_width=True)
    else:
        st.info("No DQ report yet. Run: `python src/validation/data_quality.py`")

# ──────────────────────────────────────────────────────────────
elif page == "🏗️ Architecture":
    st.markdown('<div class="section-header">🏗️ End-to-End Architecture</div>', unsafe_allow_html=True)
    st.code("""
    ┌──────────────────────────────────────────────────────────────────────┐
    │                  Employee Attendance Data Platform                    │
    ├──────────────────────────────────────────────────────────────────────┤
    │                                                                        │
    │  SOURCES            INGESTION           STORAGE           SERVING     │
    │                                                                        │
    │  Biometric ──┐      ┌──────────┐        ┌──────────┐     ┌────────┐  │
    │  Mobile App ─┼─────▶│  Kafka   │──────▶ │ Raw Lake │     │ Dash-  │  │
    │  Web Portal ─┘      │ Producer │        │ (HDFS/S3)│     │ board  │  │
    │                     └──────────┘        └────┬─────┘     │(Stream)│  │
    │  CSV Files ──┐                               │            └────────┘  │
    │  APIs ───────┼─────▶ Batch ETL ─────────────▶│                        │
    │  DB Exports ─┘       (Airflow)               ▼                        │
    │                                        ┌──────────┐     ┌────────┐   │
    │                                        │ PySpark  │────▶│ Data   │   │
    │                                        │ Process  │     │ Wareh- │   │
    │                                        └──────────┘     │ ouse   │   │
    │                                              │           │(SQLite/│   │
    │                                              ▼           │BigQry) │   │
    │                                        ┌──────────┐     └───┬────┘   │
    │                                        │ Delta    │         │        │
    │                                        │ Lakehouse│         ▼        │
    │                                        └──────────┘   ┌──────────┐  │
    │                                                        │Streamlit │  │
    │  DATA QUALITY   DQ Checks ──────────────────────────▶ │Dashboard │  │
    │  MONITORING     Anomaly Detection                      └──────────┘  │
    └──────────────────────────────────────────────────────────────────────┘
    """)

# ──────────────────────────────────────────────────────────────
elif page == "📚 Task Reference":
    st.markdown('<div class="section-header">📚 30 Task Reference</div>', unsafe_allow_html=True)
    tasks = [
        (1, "Linux + File System", "scripts/setup_linux.sh"),
        (2, "Networking/API",      "src/ingestion/api_transfer.py"),
        (3, "Python CSV/Merge",    "src/ingestion/csv_merger.py"),
        (4, "Python Modules",      "src/transformation/modules/transformers.py"),
        (5, "Pandas + NumPy",      "src/transformation/pandas_analysis.py"),
        (6, "SQL Basics",          "sql/basic_queries.sql"),
        (7, "Advanced SQL",        "sql/advanced_queries.sql"),
        (8, "OLTP vs OLAP",        "sql/setup_database.py"),
        (9, "Star Schema",         "sql/setup_database.py"),
        (10, "ETL vs ELT",         "src/ingestion/etl_elt_compare.py"),
        (11, "Batch Ingestion",    "src/ingestion/batch_ingestor.py"),
        (12, "Hadoop HDFS",        "hadoop/hdfs_setup.sh"),
        (13, "HDFS Architecture",  "hadoop/hdfs_architecture.md"),
        (14, "Spark Basics",       "spark/spark_basic.py"),
        (15, "Spark DataFrames",   "spark/spark_basic.py"),
        (16, "Spark SQL",          "spark/spark_basic.py"),
        (17, "PySpark Advanced",   "spark/spark_basic.py"),
        (18, "Streaming Concepts", "kafka/producer.py"),
        (19, "Kafka Basics",       "kafka/producer.py + consumer.py"),
        (20, "Kafka Advanced",     "kafka/advanced_kafka.py"),
        (21, "Spark Streaming",    "spark/spark_streaming.py"),
        (22, "Airflow Basics",     "airflow/dags/etl_dag.py"),
        (23, "Airflow Advanced",   "airflow/dags/etl_dag.py"),
        (24, "Cloud Basics",       "cloud/cloud_compare.md"),
        (25, "Cloud Storage",      "cloud/s3_gcs_upload.py"),
        (26, "Cloud Compute",      "cloud/s3_gcs_upload.py"),
        (27, "Cloud Warehouse",    "cloud/s3_gcs_upload.py"),
        (28, "Delta Lakehouse",    "cloud/s3_gcs_upload.py"),
        (29, "Data Quality",       "src/validation/data_quality.py"),
        (30, "Final Dashboard",    "src/dashboard/app.py"),
    ]
    tasks_df = pd.DataFrame(tasks, columns=["Task #", "Topic", "File"])
    st.dataframe(tasks_df, use_container_width=True, hide_index=True)
