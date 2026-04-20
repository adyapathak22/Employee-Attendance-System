"""
scripts/generate_data.py
Task 3, 5, 11 — Generate realistic employee attendance CSV data (1M+ rows capability)
"""
import os
import random
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")
log = logging.getLogger(__name__)

fake = Faker("en_IN")
random.seed(42)
np.random.seed(42)

DEPARTMENTS = ["Engineering", "HR", "Finance", "Sales", "Marketing", "Operations", "Legal", "IT Support"]
SHIFTS = ["Morning", "Afternoon", "Night"]
STATUSES = ["Present", "Absent", "Late", "Half-Day", "Work-From-Home", "On-Leave"]
STATUS_WEIGHTS = [0.65, 0.08, 0.10, 0.05, 0.10, 0.02]
LOCATIONS = ["Lucknow", "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"]

def generate_employees(n=200):
    employees = []
    for i in range(1, n + 1):
        dept = random.choice(DEPARTMENTS)
        join_date = fake.date_between(start_date="-5y", end_date="-6m")
        employees.append({
            "employee_id": f"EMP{i:04d}",
            "name": fake.name(),
            "email": fake.email(),
            "department": dept,
            "designation": random.choice(["Analyst", "Senior Analyst", "Manager", "Senior Manager", "Director"]),
            "location": random.choice(LOCATIONS),
            "manager_id": f"EMP{random.randint(1, min(i, 20)):04d}" if i > 20 else None,
            "join_date": join_date,
            "salary": round(random.gauss(75000, 30000), 2),
            "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
        })
    return pd.DataFrame(employees)

def generate_attendance(employees_df, days=365):
    log.info(f"Generating attendance for {len(employees_df)} employees × {days} days...")
    records = []
    start_date = datetime.now() - timedelta(days=days)

    for _, emp in employees_df.iterrows():
        for day_offset in range(days):
            date = start_date + timedelta(days=day_offset)
            if date.weekday() >= 5:  # Skip weekends
                continue
            status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
            check_in = None
            check_out = None
            hours_worked = 0.0

            if status in ("Present", "Late", "Half-Day", "Work-From-Home"):
                base_in = 9 if status != "Late" else random.randint(10, 12)
                check_in = date.replace(hour=base_in, minute=random.randint(0, 59))
                worked = random.uniform(4, 5) if status == "Half-Day" else random.uniform(7.5, 10)
                check_out = check_in + timedelta(hours=worked)
                hours_worked = round(worked, 2)

            records.append({
                "attendance_id": len(records) + 1,
                "employee_id": emp["employee_id"],
                "date": date.date(),
                "day_of_week": date.strftime("%A"),
                "shift": random.choice(SHIFTS),
                "status": status,
                "check_in": check_in.strftime("%H:%M:%S") if check_in else None,
                "check_out": check_out.strftime("%H:%M:%S") if check_out else None,
                "hours_worked": hours_worked,
                "overtime_hours": max(0, round(hours_worked - 8, 2)),
                "department": emp["department"],
                "location": emp["location"],
                # Inject some missing values for cleaning tasks
                "notes": fake.sentence() if random.random() < 0.1 else None,
            })

    return pd.DataFrame(records)

def generate_leaves(employees_df):
    records = []
    leave_types = ["Sick", "Casual", "Earned", "Maternity", "Paternity", "Unpaid"]
    for _, emp in employees_df.iterrows():
        for _ in range(random.randint(0, 6)):
            start = fake.date_between(start_date="-1y", end_date="today")
            records.append({
                "leave_id": len(records) + 1,
                "employee_id": emp["employee_id"],
                "leave_type": random.choice(leave_types),
                "start_date": start,
                "end_date": start + timedelta(days=random.randint(1, 5)),
                "approved": random.choices([True, False], weights=[0.85, 0.15])[0],
                "approved_by": f"EMP{random.randint(1, 20):04d}",
            })
    return pd.DataFrame(records)

def generate_departments():
    return pd.DataFrame([
        {"department_id": i+1, "department_name": d, "head": fake.name(), "budget": random.randint(500000, 5000000)}
        for i, d in enumerate(DEPARTMENTS)
    ])

def main():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("data/warehouse", exist_ok=True)

    log.info("Generating employees...")
    emp_df = generate_employees(200)
    emp_df.to_csv("data/raw/employees.csv", index=False)
    log.info(f"  → employees.csv: {len(emp_df)} rows")

    log.info("Generating attendance (this may take ~30s)...")
    att_df = generate_attendance(emp_df, days=365)
    att_df.to_csv("data/raw/attendance.csv", index=False)
    log.info(f"  → attendance.csv: {len(att_df)} rows")

    log.info("Generating leaves...")
    leave_df = generate_leaves(emp_df)
    leave_df.to_csv("data/raw/leaves.csv", index=False)
    log.info(f"  → leaves.csv: {len(leave_df)} rows")

    log.info("Generating departments...")
    dept_df = generate_departments()
    dept_df.to_csv("data/raw/departments.csv", index=False)
    log.info(f"  → departments.csv: {len(dept_df)} rows")

    # Corrupt ~5% for cleaning tasks
    att_df_dirty = att_df.copy()
    idx = att_df_dirty.sample(frac=0.05).index
    att_df_dirty.loc[idx, "hours_worked"] = np.nan
    att_df_dirty.loc[att_df_dirty.sample(frac=0.02).index, "status"] = None
    att_df_dirty.to_csv("data/raw/attendance_dirty.csv", index=False)
    log.info(f"  → attendance_dirty.csv: dirty version for cleaning tasks")

    log.info("✅ All data generated successfully!")

if __name__ == "__main__":
    main()
