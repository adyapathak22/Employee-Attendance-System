-- sql/advanced_queries.sql
-- TASK 7 — Advanced SQL: Joins, Subqueries, Window Functions

-- 1. WINDOW FUNCTION: Running total of hours worked per employee
SELECT
    employee_id,
    date,
    hours_worked,
    SUM(hours_worked) OVER (
        PARTITION BY employee_id ORDER BY date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_hours,
    AVG(hours_worked) OVER (
        PARTITION BY employee_id ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_avg
FROM attendance
WHERE status IN ('Present', 'Late', 'Work-From-Home')
ORDER BY employee_id, date;

-- 2. WINDOW FUNCTION: Rank employees by attendance within department
SELECT
    e.employee_id, e.name, e.department,
    COUNT(CASE WHEN a.status = 'Present' THEN 1 END) AS present_days,
    RANK() OVER (
        PARTITION BY e.department
        ORDER BY COUNT(CASE WHEN a.status = 'Present' THEN 1 END) DESC
    ) AS dept_rank,
    NTILE(4) OVER (
        ORDER BY COUNT(CASE WHEN a.status = 'Present' THEN 1 END) DESC
    ) AS performance_quartile
FROM employees e
LEFT JOIN attendance a ON e.employee_id = a.employee_id
GROUP BY e.employee_id, e.name, e.department;

-- 3. SUBQUERY: Employees with above-average overtime
SELECT e.name, e.department, stats.avg_overtime
FROM employees e
JOIN (
    SELECT employee_id, AVG(overtime_hours) AS avg_overtime
    FROM attendance
    GROUP BY employee_id
) stats ON e.employee_id = stats.employee_id
WHERE stats.avg_overtime > (
    SELECT AVG(overtime_hours) FROM attendance
)
ORDER BY stats.avg_overtime DESC;

-- 4. CTE: Month-over-month attendance change
WITH monthly AS (
    SELECT
        employee_id,
        strftime('%Y-%m', date) AS ym,
        SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) AS present_days
    FROM attendance
    GROUP BY employee_id, ym
),
with_lag AS (
    SELECT *,
        LAG(present_days) OVER (PARTITION BY employee_id ORDER BY ym) AS prev_month
    FROM monthly
)
SELECT employee_id, ym, present_days, prev_month,
       present_days - COALESCE(prev_month, present_days) AS mom_change
FROM with_lag
ORDER BY employee_id, ym;

-- 5. SELF JOIN: Find employees who were absent when their manager was present
SELECT a1.employee_id AS emp_id, a1.date,
       e.manager_id,
       a2.status AS manager_status
FROM attendance a1
JOIN employees e ON a1.employee_id = e.employee_id
JOIN attendance a2 ON e.manager_id = a2.employee_id AND a1.date = a2.date
WHERE a1.status = 'Absent'
  AND a2.status = 'Present'
ORDER BY a1.date DESC
LIMIT 100;

-- 6. PIVOT-style: Attendance heatmap by day-of-week
SELECT
    department,
    SUM(CASE WHEN day_of_week = 'Monday' THEN 1 ELSE 0 END) AS monday,
    SUM(CASE WHEN day_of_week = 'Tuesday' THEN 1 ELSE 0 END) AS tuesday,
    SUM(CASE WHEN day_of_week = 'Wednesday' THEN 1 ELSE 0 END) AS wednesday,
    SUM(CASE WHEN day_of_week = 'Thursday' THEN 1 ELSE 0 END) AS thursday,
    SUM(CASE WHEN day_of_week = 'Friday' THEN 1 ELSE 0 END) AS friday
FROM attendance
WHERE status = 'Absent'
GROUP BY department
ORDER BY department;
