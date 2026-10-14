# Databricks notebook source
from pyspark.sql import functions as F
from datetime import date

test_data = [
    # Negative booking value
    ("TEST001", -50.0, 5.0, 4.5, 4.2, date(2024, 3, 23)),

    # Duplicate Booking ID
    ("TEST001", 100.0, 3.0, 4.0, 4.0, date(2024, 3, 23)),

    # Missing Booking ID
    (None, 75.0, 2.0, 4.1, 4.4, date(2024, 3, 23)),

    # A valid control record
    ("TEST002", 120.0, 6.0, 4.8, 4.5, date(2024, 3, 23))
]

schema = """
    booking_id STRING,
    booking_value DOUBLE,
    ride_distance DOUBLE,
    driver_ratings DOUBLE,
    customer_rating DOUBLE,
    booking_date DATE
"""

test_df = spark.createDataFrame(test_data, schema)

display(test_df)

# COMMAND ----------

negative_values = test_df.filter(
    F.col("booking_value") < 0
).count()

missing_booking_ids = test_df.filter(
    F.col("booking_id").isNull()
).count()

duplicate_booking_ids = (
    test_df
    .filter(F.col("booking_id").isNotNull())
    .groupBy("booking_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

invalid_driver_ratings = test_df.filter(
    (F.col("driver_ratings") < 1) |
    (F.col("driver_ratings") > 5)
).count()

invalid_customer_ratings = test_df.filter(
    (F.col("customer_rating") < 1) |
    (F.col("customer_rating") > 5)
).count()

checks = [
    ("Negative booking values", negative_values),
    ("Missing Booking IDs", missing_booking_ids),
    ("Duplicate Booking IDs", duplicate_booking_ids),
    ("Invalid driver ratings", invalid_driver_ratings),
    ("Invalid customer ratings", invalid_customer_ratings)
]

for check_name, issue_count in checks:
    status = "FAIL" if issue_count > 0 else "PASS"
    print(f"{check_name}: {status} | Issues detected: {issue_count}")

# COMMAND ----------

critical_failures = [
    name
    for name, issue_count in checks
    if issue_count > 0
]

if critical_failures:
    decision = "NEEDS_ATTENTION"
    next_action = "STOP and investigate bad records"
else:
    decision = "PROCEED"
    next_action = "Continue to analytics"

print("Agent decision:", decision)
print("Critical failures:", critical_failures)
print("Next action:", next_action)

assert decision == "NEEDS_ATTENTION"

# COMMAND ----------

from pyspark.sql import functions as F

rating_test_data = [
    ("TEST101", 4.5, 4.0),
    ("TEST102", 5.5, 4.2),  # Invalid driver rating
    ("TEST103", 4.0, 6.0),  # Invalid customer rating
    ("TEST104", 3.5, 3.8)
]

rating_df = spark.createDataFrame(
    rating_test_data,
    ["booking_id", "driver_ratings", "customer_rating"]
)

display(rating_df)

# COMMAND ----------

invalid_driver = rating_df.filter(
    F.col("driver_ratings").isNotNull() &
    ((F.col("driver_ratings") < 1) | (F.col("driver_ratings") > 5))
).count()

invalid_customer = rating_df.filter(
    F.col("customer_rating").isNotNull() &
    ((F.col("customer_rating") < 1) | (F.col("customer_rating") > 5))
).count()

print("Invalid driver ratings:", invalid_driver)
print("Invalid customer ratings:", invalid_customer)

assert invalid_driver == 1
assert invalid_customer == 1

print("PASS: Both invalid ratings were detected.")

# COMMAND ----------

