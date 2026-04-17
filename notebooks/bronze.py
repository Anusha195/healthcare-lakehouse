import dlt
from pyspark.sql.functions import *

# Appointments
@dlt.table
def bronze_appointments():
    return spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("/Volumes/workspace/default/healthcaredata/appointment") \
        .withColumn("PatientId", col("PatientId").cast("string")) \
        .withColumn("AppointmentID", col("AppointmentID").cast("string"))


# Doctors
@dlt.table
def bronze_doctors():
    return spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("/Volumes/workspace/default/healthcaredata/doctors.csv")


# Departments
@dlt.table
def bronze_departments():
    return spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("/Volumes/workspace/default/healthcaredata/departments.csv")

# billings
@dlt.table
def bronze_billings():
    return spark.read.format("csv") \
        .option("header", True) \
        .load("/Volumes/workspace/default/healthcaredata/billings") \
        .withColumn("appointment_id", col("appointment_id").cast("string")) \
        .withColumn("patient_id", col("patient_id").cast("string"))

# Diagnostics
@dlt.table
def bronze_diagnostics():
    return spark.read.format("csv") \
        .option("header", True) \
        .load("/Volumes/workspace/default/healthcaredata/diagnostics") \
        .withColumn("appointment_id", col("appointment_id").cast("string")) \
        .withColumn("diagnostic_id", col("diagnostic_id").cast("string"))



