-- sql/basic_queries.sql
-- TASK 6 — SQL Basics: SELECT, WHERE, GROUP BY, ORDER BY

-- 1. All employees in Engineering department
SELECT employee_id, name, designation, salary
FROM employees
WHERE department = 'Engineering'
ORDER BY salary DESC;

-- 2. Attendance count per status
SELECT status, COUNT(*) AS total_records,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM attendance
GROUP BY status
ORDER BY total_records DESC;

-- 3. Top 10 employees by hours worked this month
SELECT e.employee_id, e.name, e.department,
       SUM(a.hours_worked) AS total_hours,
       COUNT(a.attendance_id) AS working_days
FROM employees e
JOIN attendance a ON e.employee_id = a.employee_id
WHERE strftime('%Y-%m', a.date) = strftime('%Y-%m', 'now')
GROUP BY e.employee_id, e.name, e.department
ORDER BY total_hours DESC
LIMIT 10;

-- 4. Departments with attendance rate below 80%
SELECT department,
       COUNT(*) AS total_records,
       SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) AS present_count,
       ROUND(100.0 * SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) / COUNT(*), 2) AS attendance_pct
FROM attendance
GROUP BY department
HAVING attendance_pct < 80
ORDER BY attendance_pct ASC;

-- 5. Daily attendance for last 30 days
SELECT date, COUNT(DISTINCT employee_id) AS employees_present,
       AVG(hours_worked) AS avg_hours
FROM attendance
WHERE status = 'Present'
  AND date >= date('now', '-30 days')
GROUP BY date
ORDER BY date DESC;
