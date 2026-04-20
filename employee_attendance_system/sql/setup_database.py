"""
sql/setup_database.py
TASKS 6, 7, 8, 9 — Database setup: OLTP schema + star schema + run all queries
"""
import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

DB_PATH = "data/attendance.db"


def get_conn():
    return sqlite3.connect(DB_PATH)


# ──────────────────────────────────────────────────────────────
# TASK 8: OLTP Schema (Transactional System)
# ──────────────────────────────────────────────────────────────
OLTP_DDL = """
CREATE TABLE IF NOT EXISTS departments (
    department_id   INTEGER PRIMARY KEY,
    department_name TEXT NOT NULL UNIQUE,
    head            TEXT,
    budget          REAL
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id   TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    email         TEXT UNIQUE,
    department    TEXT REFERENCES departments(department_name),
    designation   TEXT,
    location      TEXT,
    manager_id    TEXT REFERENCES employees(employee_id),
    join_date     DATE,
    salary        REAL,
    is_active     BOOLEAN DEFAULT 1
);

CREATE TABLE IF NOT EXISTS attendance (
    attendance_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id     TEXT NOT NULL REFERENCES employees(employee_id),
    date            DATE NOT NULL,
    day_of_week     TEXT,
    shift           TEXT CHECK(shift IN ('Morning','Afternoon','Night')),
    status          TEXT CHECK(status IN ('Present','Absent','Late','Half-Day','Work-From-Home','On-Leave','Unknown')),
    check_in        TEXT,
    check_out       TEXT,
    hours_worked    REAL DEFAULT 0 CHECK(hours_worked >= 0),
    overtime_hours  REAL DEFAULT 0 CHECK(overtime_hours >= 0),
    department      TEXT,
    location        TEXT,
    notes           TEXT,
    UNIQUE(employee_id, date)
);

CREATE TABLE IF NOT EXISTS leaves (
    leave_id      INTEGER PRIMARY KEY,
    employee_id   TEXT NOT NULL REFERENCES employees(employee_id),
    leave_type    TEXT,
    start_date    DATE,
    end_date      DATE,
    approved      BOOLEAN,
    approved_by   TEXT
);

CREATE INDEX IF NOT EXISTS idx_att_emp ON attendance(employee_id);
CREATE INDEX IF NOT EXISTS idx_att_date ON attendance(date);
CREATE INDEX IF NOT EXISTS idx_att_dept ON attendance(department);
CREATE INDEX IF NOT EXISTS idx_att_status ON attendance(status);
"""

# ──────────────────────────────────────────────────────────────
# TASK 9: Star Schema (OLAP / Data Warehouse)
# ──────────────────────────────────────────────────────────────
STAR_SCHEMA_DDL = """
-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER PRIMARY KEY,  -- YYYYMMDD
    full_date     DATE,
    day_of_week   TEXT,
    day_num       INTEGER,
    week_num      INTEGER,
    month_num     INTEGER,
    month_name    TEXT,
    quarter       INTEGER,
    year          INTEGER,
    is_weekend    BOOLEAN,
    is_holiday    BOOLEAN DEFAULT 0
);

-- Dimension: Employee
CREATE TABLE IF NOT EXISTS dim_employee (
    emp_key       INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_id   TEXT,
    name          TEXT,
    department    TEXT,
    designation   TEXT,
    location      TEXT,
    salary_band   TEXT,
    effective_from DATE,
    effective_to   DATE,
    is_current    BOOLEAN DEFAULT 1
);

-- Dimension: Department
CREATE TABLE IF NOT EXISTS dim_department (
    dept_key      INTEGER PRIMARY KEY AUTOINCREMENT,
    department    TEXT,
    head          TEXT,
    budget_tier   TEXT
);

-- Dimension: Shift
CREATE TABLE IF NOT EXISTS dim_shift (
    shift_key   INTEGER PRIMARY KEY,
    shift_name  TEXT,
    start_time  TEXT,
    end_time    TEXT
);

-- Fact: Attendance
CREATE TABLE IF NOT EXISTS fact_attendance (
    fact_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date_key        INTEGER REFERENCES dim_date(date_key),
    emp_key         INTEGER REFERENCES dim_employee(emp_key),
    dept_key        INTEGER REFERENCES dim_department(dept_key),
    shift_key       INTEGER REFERENCES dim_shift(shift_key),
    status          TEXT,
    hours_worked    REAL,
    overtime_hours  REAL,
    is_present      INTEGER,
    is_absent       INTEGER,
    is_late         INTEGER,
    is_wfh          INTEGER
);
"""


def load_csv_to_db(conn):
    """Load CSV data into OLTP tables."""
    tables = {
        "departments": "data/raw/departments.csv",
        "employees": "data/raw/employees.csv",
        "attendance": "data/raw/attendance.csv",
        "leaves": "data/raw/leaves.csv",
    }
    for table, path in tables.items():
        if not os.path.exists(path):
            log.warning(f"  Skipping {table} — file not found: {path}")
            continue
        df = pd.read_csv(path, low_memory=False)
        df.to_sql(table, conn, if_exists="replace", index=False)
        log.info(f"  Loaded {len(df):,} rows → {table}")


def populate_star_schema(conn):
    """Populate OLAP star schema from OLTP data."""
    log.info("Populating star schema...")

    try:
        att_df = pd.read_sql("SELECT * FROM attendance", conn)
        emp_df = pd.read_sql("SELECT * FROM employees", conn)
        dept_df = pd.read_sql("SELECT * FROM departments", conn)
    except Exception:
        log.warning("OLTP tables not loaded. Skipping star schema population.")
        return

    # dim_date
    dates = pd.to_datetime(att_df["date"].unique())
    dim_date = pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.date,
        "day_of_week": dates.strftime("%A"),
        "day_num": dates.day,
        "week_num": dates.isocalendar().week,
        "month_num": dates.month,
        "month_name": dates.strftime("%B"),
        "quarter": dates.quarter,
        "year": dates.year,
        "is_weekend": dates.weekday >= 5,
    })
    dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
    log.info(f"  dim_date: {len(dim_date)} rows")

    # dim_employee
    dim_emp = emp_df[["employee_id", "name", "department", "designation", "location", "salary"]].copy()
    dim_emp["salary_band"] = pd.cut(
        dim_emp["salary"], bins=[0, 40000, 80000, 150000, float("inf")],
        labels=["Junior", "Mid", "Senior", "Executive"]
    )
    dim_emp["effective_from"] = emp_df["join_date"]
    dim_emp["effective_to"] = None
    dim_emp["is_current"] = 1
    dim_emp.to_sql("dim_employee", conn, if_exists="replace", index=False)
    log.info(f"  dim_employee: {len(dim_emp)} rows")

    # dim_department
    dim_dept = dept_df[["department_name", "head", "budget"]].copy()
    dim_dept.columns = ["department", "head", "budget"]
    dim_dept["budget_tier"] = pd.cut(
        dim_dept["budget"], bins=[0, 1000000, 3000000, float("inf")],
        labels=["Small", "Medium", "Large"]
    )
    dim_dept.to_sql("dim_department", conn, if_exists="replace", index=False)
    log.info(f"  dim_department: {len(dim_dept)} rows")

    # dim_shift
    dim_shift = pd.DataFrame([
        {"shift_key": 1, "shift_name": "Morning",   "start_time": "06:00", "end_time": "14:00"},
        {"shift_key": 2, "shift_name": "Afternoon",  "start_time": "14:00", "end_time": "22:00"},
        {"shift_key": 3, "shift_name": "Night",      "start_time": "22:00", "end_time": "06:00"},
    ])
    dim_shift.to_sql("dim_shift", conn, if_exists="replace", index=False)

    log.info("  Star schema populated!")


def run_sample_queries(conn):
    log.info("\n── Sample Queries ──────────────────────────────────────")
    queries = {
        "Attendance by department": """
            SELECT department, status, COUNT(*) as count
            FROM attendance GROUP BY department, status
            ORDER BY department, count DESC LIMIT 20
        """,
        "Top 5 overtime employees": """
            SELECT e.name, e.department, SUM(a.overtime_hours) as total_ot
            FROM employees e JOIN attendance a ON e.employee_id = a.employee_id
            GROUP BY e.employee_id ORDER BY total_ot DESC LIMIT 5
        """,
        "Monthly attendance trend": """
            SELECT strftime('%Y-%m', date) as month,
                   SUM(CASE WHEN status='Present' THEN 1 ELSE 0 END) as present_count
            FROM attendance GROUP BY month ORDER BY month
        """,
    }
    for name, sql in queries.items():
        try:
            result = pd.read_sql(sql.strip(), conn)
            log.info(f"\n{name}:\n{result.head(5).to_string(index=False)}")
        except Exception as e:
            log.warning(f"  Query '{name}' failed: {e}")


def main():
    log.info("=== Tasks 6–9: SQL Database Setup ===\n")
    os.makedirs("data", exist_ok=True)

    with get_conn() as conn:
        log.info("Creating OLTP schema (Task 8)...")
        conn.executescript(OLTP_DDL)
        log.info("Creating star schema (Task 9)...")
        conn.executescript(STAR_SCHEMA_DDL)
        conn.commit()

        log.info("Loading data into OLTP tables...")
        load_csv_to_db(conn)

        populate_star_schema(conn)
        run_sample_queries(conn)

    log.info(f"\n✅ Database ready at: {DB_PATH}")


if __name__ == "__main__":
    main()
