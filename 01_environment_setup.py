# Databricks notebook source

# Step 1: Verify that PySpark is working

print("Starting PySpark test...")

# Create a small Spark DataFrame
df = spark.range(1, 6)

# Display the data
display(df)

# Count the rows
print("Total rows:", df.count())


# COMMAND ----------


# Check the current Databricks environment
print("Current database:", spark.catalog.currentDatabase())

# Check whether the file is accessible through the common
# Databricks workspace file path
display(dbutils.fs.ls("file:/Workspace/Users/tanishpriyam@gmail.com/ncr_ride_bookings.csv"))


# COMMAND ----------


# Step 1: Define the CSV file path
file_path = "file:/Workspace/Users/tanishpriyam@gmail.com/ncr_ride_bookings.csv"

# Step 2: Read the CSV file into a Spark DataFrame
df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(file_path)
)

print("CSV loaded successfully!")


# COMMAND ----------


# Display the first 10 records
display(df.limit(10))

# Count the records
print("Total records:", df.count())

# Count the columns
print("Total columns:", len(df.columns))

# Inspect column names and data types
df.printSchema()


# COMMAND ----------


print("Column names:")
for column_name in df.columns:
    print(column_name)


# COMMAND ----------


# Step 1A: Check the source data

print("Total records:", df.count())
print("Total columns:", len(df.columns))

display(df.limit(10))

# Check missing values in each column
from pyspark.sql import functions as F

missing_counts = df.select([
    F.sum(
        F.when(
            F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in df.columns
])

display(missing_counts)


# COMMAND ----------


# Step 2A: Import the required libraries

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone
import uuid

# Step 2B: Define the source file path

file_path = "file:/Workspace/Users/tanishpriyam@gmail.com/ncr_ride_bookings.csv"

# Step 2C: Define the exact 21-column source schema
source_columns = [
    "Date",
    "Time",
    "Booking ID",
    "Booking Status",
    "Customer ID",
    "Vehicle Type",
    "Pickup Location",
    "Drop Location",
    "Avg VTAT",
    "Avg CTAT",
    "Cancelled Rides by Customer",
    "Reason for cancelling by Customer",
    "Cancelled Rides by Driver",
    "Driver Cancellation Reason",
    "Incomplete Rides",
    "Incomplete Rides Reason",
    "Booking Value",
    "Ride Distance",
    "Driver Ratings",
    "Customer Rating",
    "Payment Method"
]

source_schema = StructType([
    StructField(column_name, StringType(), True)
    for column_name in source_columns
])

# Step 2D: Read the CSV using the explicit schema

raw_df = (
    spark.read
    .format("csv")
    .schema(source_schema)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("encoding", "UTF-8")
    .load(file_path)
)

# Step 2E: Add ingestion metadata

batch_id = str(uuid.uuid4())

bronze_df = (
    raw_df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.lit("ncr_ride_bookings.csv"))
    .withColumn("batch_id", F.lit(batch_id))
)

print("Bronze DataFrame created.")
print("Source columns:", len(source_columns))
print("Bronze columns:", len(bronze_df.columns))
print("Batch ID:", batch_id)

display(bronze_df.limit(10))


# COMMAND ----------


from pyspark.sql import functions as F
import re

# Use the Bronze DataFrame created in Step 2
# Keep the original column names available for reference.

def clean_column_name(name):
    name = name.strip()
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")

# Rename source columns to Delta-compatible names
renamed_df = bronze_df

for old_name in bronze_df.columns:
    new_name = clean_column_name(old_name)
    if old_name != new_name:
        renamed_df = renamed_df.withColumnRenamed(old_name, new_name)

# Check for duplicate column names after cleaning
if len(renamed_df.columns) != len(set(renamed_df.columns)):
    raise ValueError("Duplicate column names detected after renaming.")

# Save as a Delta table
bronze_table = "bronze_ncr_ride_bookings"

(
    renamed_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print("Bronze table saved successfully!")
print("Total columns:", len(renamed_df.columns))
print("Total records:", renamed_df.count())

display(renamed_df.limit(10))


# COMMAND ----------


bronze = spark.table("bronze_ncr_ride_bookings")

print("Bronze record count:", bronze.count())
print("Bronze column count:", len(bronze.columns))
print("Bronze columns:")

for column_name in bronze.columns:
    print(column_name)

display(bronze.limit(10))


# COMMAND ----------


from pyspark.sql import functions as F

# Always rebuild from Bronze to avoid reapplying transformations
bronze = spark.table("bronze_ncr_ride_bookings")
silver = bronze

# Clean blank strings and textual NULL values
text_columns = [
    field.name
    for field in bronze.schema.fields
    if field.dataType.simpleString() == "string"
]

for column_name in text_columns:
    silver = silver.withColumn(
        column_name,
        F.when(
            F.col(column_name).isNull()
            | (F.trim(F.col(column_name)) == "")
            | (F.upper(F.trim(F.col(column_name))) == "NULL"),
            F.lit(None).cast("string")
        ).otherwise(F.trim(F.col(column_name)))
    )

# Parse the date using the format observed in your dataset
silver = silver.withColumn(
    "booking_date",
    F.expr("try_to_date(`Date`, 'yyyy-MM-dd')")
)

# Parse time and combine it with the parsed date
silver = silver.withColumn(
    "booking_time",
    F.expr("""
        try_to_timestamp(
            concat(
                date_format(booking_date, 'yyyy-MM-dd'),
                ' ',
                `Time`
            ),
            'yyyy-MM-dd HH:mm:ss'
        )
    """)
)

# Derive date and time analytics fields
silver = (
    silver
    .withColumn("booking_hour", F.hour("booking_time"))
    .withColumn("day_of_week", F.date_format("booking_date", "EEEE"))
)

# Convert numeric fields safely
numeric_columns = {
    "Avg_VTAT": "DOUBLE",
    "Avg_CTAT": "DOUBLE",
    "Booking_Value": "DOUBLE",
    "Ride_Distance": "DOUBLE",
    "Driver_Ratings": "DOUBLE",
    "Customer_Rating": "DOUBLE"
}

for column_name, data_type in numeric_columns.items():
    silver = silver.withColumn(
        column_name,
        F.expr(f"try_cast(`{column_name}` AS {data_type})")
    )

# Standardize common categorical fields
for column_name in [
    "Booking_Status",
    "Vehicle_Type",
    "Payment_Method"
]:
    silver = silver.withColumn(
        column_name,
        F.lower(F.col(column_name))
    )

# Derive cancellation indicator
silver = silver.withColumn(
    "is_cancelled",
    F.when(
        F.col("Booking_Status").contains("cancel"),
        F.lit(True)
    ).otherwise(F.lit(False))
)

print("Silver DataFrame prepared successfully.")
display(
    silver.select(
        "Date", "Time", "booking_date", "booking_time",
        "booking_hour", "Booking_Status", "Booking_Value"
    ).limit(10)
)


# COMMAND ----------


print("Total Bronze records:", bronze.count())
print("Total Silver records:", silver.count())

print(
    "Unparsed non-null dates:",
    silver.filter(
        F.col("Date").isNotNull()
        & F.col("booking_date").isNull()
    ).count()
)

print(
    "Unparsed timestamps:",
    silver.filter(
        F.col("Date").isNotNull()
        & F.col("Time").isNotNull()
        & F.col("booking_time").isNull()
    ).count()
)

display(
    silver.groupBy("Booking_Status")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------


from pyspark.sql import functions as F

# 1. Check duplicate Booking IDs
duplicate_ids = (
    silver.groupBy("Booking_ID")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate Booking IDs:", duplicate_ids.count())

# 2. Check missing Booking IDs
print(
    "Missing Booking IDs:",
    silver.filter(
        F.col("Booking_ID").isNull()
        | (F.trim(F.col("Booking_ID")) == "")
    ).count()
)

# 3. Check negative booking values
print(
    "Negative Booking Values:",
    silver.filter(F.col("Booking_Value") < 0).count()
)

# 4. Check negative ride distances
print(
    "Negative Ride Distances:",
    silver.filter(F.col("Ride_Distance") < 0).count()
)

# 5. Check ratings outside the range 1 to 5
print(
    "Invalid Driver Ratings:",
    silver.filter(
        F.col("Driver_Ratings").isNotNull()
        & (
            (F.col("Driver_Ratings") < 1)
            | (F.col("Driver_Ratings") > 5)
        )
    ).count()
)

print(
    "Invalid Customer Ratings:",
    silver.filter(
        F.col("Customer_Rating").isNotNull()
        & (
            (F.col("Customer_Rating") < 1)
            | (F.col("Customer_Rating") > 5)
        )
    ).count()
)


# COMMAND ----------


# Inspect the actual booking status values
display(
    silver.groupBy("Booking_Status")
    .count()
    .orderBy(F.desc("count"))
)

# Inspect cancellation flags
display(
    silver.groupBy(
        "Cancelled_Rides_by_Customer",
        "Cancelled_Rides_by_Driver"
    )
    .count()
    .orderBy(F.desc("count"))
)

# Inspect incomplete ride flags
display(
    silver.groupBy("Incomplete_Rides")
    .count()
    .orderBy(F.desc("count"))
)

# Inspect available cancellation reasons
display(
    silver.groupBy("Reason_for_cancelling_by_Customer")
    .count()
    .orderBy(F.desc("count"))
)

display(
    silver.groupBy("Driver_Cancellation_Reason")
    .count()
    .orderBy(F.desc("count"))
)


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = silver  # Use the Silver DataFrame already prepared

duplicate_ids = (
    silver.groupBy("Booking_ID")
    .count()
    .filter(F.col("count") > 1)
)

print("Booking IDs appearing more than once:", duplicate_ids.count())

print(
    "Extra rows beyond one record per duplicated ID:",
    duplicate_ids.select(
        F.sum(F.col("count") - 1).alias("extra_rows")
    ).first()["extra_rows"]
)

# Inspect sample duplicated bookings
display(
    silver.join(
        duplicate_ids.select("Booking_ID"),
        on="Booking_ID",
        how="inner"
    )
    .orderBy("Booking_ID")
    .limit(30)
)


# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Start from the prepared Silver DataFrame
work_df = silver

# Build validation conditions
missing_id = (
    F.col("Booking_ID").isNull()
    | (F.trim(F.col("Booking_ID")) == "")
)

invalid_date = F.col("booking_date").isNull()

negative_value = F.col("Booking_Value") < 0
negative_distance = F.col("Ride_Distance") < 0

invalid_driver_rating = (
    F.col("Driver_Ratings").isNotNull()
    & (
        (F.col("Driver_Ratings") < 1)
        | (F.col("Driver_Ratings") > 5)
    )
)

invalid_customer_rating = (
    F.col("Customer_Rating").isNotNull()
    & (
        (F.col("Customer_Rating") < 1)
        | (F.col("Customer_Rating") > 5)
    )
)

# Outcome consistency checks based on the observed source values
status = F.lower(F.trim(F.col("Booking_Status")))
customer_flag = F.col("Cancelled_Rides_by_Customer").cast("int")
driver_flag = F.col("Cancelled_Rides_by_Driver").cast("int")
incomplete_flag = F.col("Incomplete_Rides").cast("int")

contradictory_outcome = (
    (
        (status == "cancelled by customer")
        & (
            (customer_flag != 1)
            | driver_flag.eqNullSafe(F.lit(1))
            | incomplete_flag.eqNullSafe(F.lit(1))
        )
    )
    | (
        (status == "cancelled by driver")
        & (
            (driver_flag != 1)
            | customer_flag.eqNullSafe(F.lit(1))
            | incomplete_flag.eqNullSafe(F.lit(1))
        )
    )
    | (
        (status == "incomplete")
        & (
            (incomplete_flag != 1)
            | customer_flag.eqNullSafe(F.lit(1))
            | driver_flag.eqNullSafe(F.lit(1))
        )
    )
    | (
        (status == "completed")
        & (
            customer_flag.eqNullSafe(F.lit(1))
            | driver_flag.eqNullSafe(F.lit(1))
            | incomplete_flag.eqNullSafe(F.lit(1))
        )
    )
)

# Create a combined validation reason
work_df = work_df.withColumn(
    "validation_reason",
    F.concat_ws(
        "; ",
        F.when(missing_id, "Missing Booking_ID"),
        F.when(invalid_date, "Invalid or missing booking date"),
        F.when(negative_value, "Negative Booking_Value"),
        F.when(negative_distance, "Negative Ride_Distance"),
        F.when(invalid_driver_rating, "Driver rating outside 1-5"),
        F.when(invalid_customer_rating, "Customer rating outside 1-5"),
        F.when(contradictory_outcome, "Contradictory booking outcome")
    )
)

# Separate records that fail basic validation
invalid_df = work_df.filter(F.col("validation_reason") != "")
candidate_df = work_df.filter(F.col("validation_reason") == "")

# Identify duplicate occurrences among otherwise valid records
# Prefer the latest ingestion timestamp; break ties using a row fingerprint.
business_columns = [
    c for c in candidate_df.columns
    if c not in ["ingestion_timestamp", "source_file", "batch_id"]
]

candidate_df = candidate_df.withColumn(
    "_row_fingerprint",
    F.sha2(
        F.to_json(F.struct(*[F.col(c) for c in business_columns])),
        256
    )
)

dedup_window = Window.partitionBy("Booking_ID").orderBy(
    F.col("ingestion_timestamp").desc_nulls_last(),
    F.col("_row_fingerprint").asc()
)

ranked_df = candidate_df.withColumn(
    "_duplicate_rank",
    F.row_number().over(dedup_window)
)

duplicate_quarantine = (
    ranked_df
    .filter(F.col("_duplicate_rank") > 1)
    .withColumn("validation_reason", F.lit("Duplicate Booking_ID"))
    .drop("_duplicate_rank", "_row_fingerprint")
)

silver_final = (
    ranked_df
    .filter(F.col("_duplicate_rank") == 1)
    .drop("_duplicate_rank", "_row_fingerprint", "validation_reason")
)

# Combine invalid records and duplicate extras into quarantine
quarantine_df = (
    invalid_df
    .unionByName(duplicate_quarantine, allowMissingColumns=True)
    .withColumnRenamed("validation_reason", "quarantine_reason")
)

# Persist both tables
silver_table = "silver_ncr_ride_bookings"
quarantine_table = "quarantine_ncr_ride_bookings"

(
    silver_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(quarantine_table)
)

print("Silver table saved:", silver_table)
print("Quarantine table saved:", quarantine_table)


# COMMAND ----------


bronze_count = spark.table("bronze_ncr_ride_bookings").count()
silver_count = spark.table("silver_ncr_ride_bookings").count()
quarantine_count = spark.table("quarantine_ncr_ride_bookings").count()

print("Bronze records:", bronze_count)
print("Silver valid records:", silver_count)
print("Quarantined records:", quarantine_count)
print("Reconciled total:", silver_count + quarantine_count)

print(
    "Reconciliation passed:",
    bronze_count == silver_count + quarantine_count
)

display(
    spark.table("quarantine_ncr_ride_bookings")
    .groupBy("quarantine_reason")
    .count()
    .orderBy(F.desc("count"))
)

display(spark.table("silver_ncr_ride_bookings").limit(10))
