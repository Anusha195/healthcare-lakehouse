CREATE MATERIALIZED VIEW gold_departments_ranking
AS
SELECT *,
       RANK() OVER (ORDER BY total_revenue DESC) AS rank
FROM (
    SELECT department_name,
           SUM(amount) AS total_revenue
    FROM LIVE.silver_joined
    GROUP BY department_name
) t;

CREATE MATERIALIZED VIEW gold_running_total
AS
SELECT 
    appointment_date,
    SUM(daily_revenue) OVER (ORDER BY appointment_date) AS running_total
FROM (
    SELECT DATE(appointment_day) AS appointment_date,
           SUM(amount) AS daily_revenue
    FROM LIVE.silver_joined
    GROUP BY DATE(appointment_day)
) t;


CREATE MATERIALIZED VIEW gold_top_doctors
AS
SELECT *
FROM (
    SELECT 
        department_name,
        doctor_id,
        COUNT(appointment_id) AS total_appointments,
        RANK() OVER (
            PARTITION BY department_name 
            ORDER BY COUNT(appointment_id) DESC
        ) AS rank
    FROM LIVE.silver_joined
    GROUP BY department_name, doctor_id
) t
WHERE rank <= 3;


CREATE MATERIALIZED VIEW gold_revenue_contribution
AS
SELECT 
    department_name,
    total_revenue,
    ROUND(
        total_revenue * 100 / SUM(total_revenue) OVER (), 
        2
    ) AS contribution_percent
FROM (
    SELECT 
        department_name,
        SUM(amount) AS total_revenue
    FROM LIVE.silver_joined
    GROUP BY department_name
) t;

CREATE MATERIALIZED VIEW gold_patient_ranking
AS
SELECT 
    patient_id,
    visit_count,
    RANK() OVER (ORDER BY visit_count DESC) AS rank
FROM (
    SELECT 
        patient_id,
        COUNT(appointment_id) AS visit_count
    FROM LIVE.silver_joined
    GROUP BY patient_id
) t;

CREATE MATERIALIZED VIEW gold_leadtime_noshow
AS
SELECT 
    lead_bucket,
    ROUND(SUM(no_show)/COUNT(*), 2) AS no_show_rate
FROM (
    SELECT 
        CASE 
            WHEN DATEDIFF(appointment_day, scheduled_day) = 0 THEN 'Same Day'
            WHEN DATEDIFF(appointment_day, scheduled_day) <= 3 THEN 'Short Wait'
            ELSE 'Long Wait'
        END AS lead_bucket,
        no_show
    FROM LIVE.silver_joined
) t
GROUP BY lead_bucket;