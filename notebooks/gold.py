import dlt
from pyspark.sql.functions import *

# Montly revenue analysis
@dlt.table(
    partition_cols=["appointment_month"]
)
def gold_monthly_revenue():

    df = dlt.read("silver_joined")

    return (
        df
        .withColumn("appointment_month", month("appointment_day"))

        .groupBy("appointment_month", "department_name")
        .agg(
            sum("amount").alias("total_revenue")
        )
    )

# Revenue by department
@dlt.table
def gold_revenue_by_department():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("department_name")
        .agg(
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_revenue"),
            count("appointment_id").alias("total_appointments")
        )
    )

# Revisit patients summary
@dlt.table
def gold_revisit_patients():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("patient_id")
        .agg(
            count("appointment_id").alias("visit_count")
        )
        .filter(col("visit_count") > 1)
    )

# No-show rate by department
@dlt.table
def gold_no_show():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("department_name")
        .agg(
            (sum("no_show") / count("*")).alias("no_show_rate")
        )
    )

# Doctor utilization report
@dlt.table
def gold_doctor_utilization():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("doctor_id")
        .agg(
            count("appointment_id").alias("total_appointments")
        )
    )

# monthly trend
@dlt.table
def gold_monthly_trend():

    df = dlt.read("silver_joined")

    return (
        df
        .withColumn("appointment_month", month("appointment_day"))
        .groupBy("appointment_month")
        .agg(
            count("appointment_id").alias("total_appointments")
        )
    )

# Daily / monthly appointments trend
@dlt.table
def gold_daily_trend():

    df = dlt.read("silver_joined")

    return (
        df
        .withColumn("appointment_date", to_date("appointment_day"))
        .groupBy("appointment_date")
        .agg(
            count("appointment_id").alias("total_appointments")
        )
    )

# revenue by doctor
@dlt.table
def gold_revenue_by_doctor():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("doctor_id")
        .agg(
            sum("amount").alias("doctor_revenue")
        )
    )

# Age-group patient analysis
@dlt.table
def gold_age_analysis():

    df = dlt.read("silver_joined")

    return (
        df
        .withColumn("age_group",
            when(col("age") < 18, "Child")
            .when(col("age") < 40, "Adult")
            .when(col("age") < 60, "Middle Age")
            .otherwise("Senior")
        )
        .groupBy("age_group")
        .agg(count("*").alias("total_patients"))
    )

# City / neighbourhood demand
@dlt.table
def gold_city_demand():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("Neighbourhood")
        .agg(count("appointment_id").alias("total_appointments"))
    )

# Lead Time Analysis
@dlt.table
def gold_lead_time():

    df = dlt.read("silver_joined")

    return (
        df
        .withColumn("lead_days",
            datediff("appointment_day", "scheduled_day")
        )
        .groupBy()
        .agg(avg("lead_days").alias("avg_lead_time"))
    )

# Delay / Same-Day Booking Analysis
@dlt.table
def gold_booking_behavior():

    df = dlt.read("silver_joined")

    return (
        df
        .withColumn("lead_days",
            datediff("appointment_day", "scheduled_day")
        )
        .withColumn("booking_type",
            when(col("lead_days") == 0, "Same Day")
            .when(col("lead_days") <= 3, "Short Term")
            .otherwise("Advance Booking")
        )
        .groupBy("booking_type")
        .agg(count("*").alias("total"))
    )

# Daily No-show Trend
@dlt.table
def gold_daily_no_show():

    df = dlt.read("silver_joined")

    return (
        df
        .withColumn("appointment_date", to_date("appointment_day"))
        .groupBy("appointment_date")
        .agg(
            (sum("no_show") / count("*")).alias("no_show_rate")
        )
    )


# Executive KPI table
@dlt.table
def gold_executive_kpi():

    df = dlt.read("silver_joined")

    return df.agg(
        count("appointment_id").alias("total_appointments"),
        sum("amount").alias("total_revenue"),
        avg("age").alias("avg_age"),
        (sum("no_show") / count("*")).alias("no_show_rate")
    )

# Payment Status Analysis
@dlt.table
def gold_payment_status():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("payment_status")
        .agg(
            count("appointment_id").alias("total"),
            sum("amount").alias("total_amount")
        )
    )

# Revenue Loss (Pending Payments)
@dlt.table
def gold_revenue_loss():

    df = dlt.read("silver_joined")

    return (
        df
        .filter(col("payment_status") == "Pending")
        .agg(
            sum("amount").alias("pending_revenue")
        )
    )
    
# Test Popularity (Diagnostics)
@dlt.table
def gold_test_popularity():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("test_name")
        .agg(
            count("*").alias("total_tests")
        )
    )

# Abnormal vs Normal Results
@dlt.table
def gold_test_results():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("test_name", "result")
        .agg(
            count("*").alias("count")
        )
    )

# Revenue by Test Type
@dlt.table
def gold_revenue_by_test():

    df = dlt.read("silver_joined")

    return (
        df
        .groupBy("test_name")
        .agg(
            sum("amount").alias("total_revenue")
        )
    )

