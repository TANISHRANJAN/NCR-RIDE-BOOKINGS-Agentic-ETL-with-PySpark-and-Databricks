# Databricks notebook source
from pyspark.sql import functions as F

bronze_count = spark.table("bronze_ncr_ride_bookings").count()
silver_count = spark.table("silver_ncr_ride_bookings").count()
quarantine_count = spark.table("quarantine_ncr_ride_bookings").count()

print("Bronze records:", bronze_count)
print("Silver records:", silver_count)
print("Quarantined records:", quarantine_count)
print("Silver + Quarantine:", silver_count + quarantine_count)

reconciliation_passed = (
    bronze_count == silver_count + quarantine_count
)

print("Reconciliation passed:", reconciliation_passed)

assert reconciliation_passed, (
    "FAILED: Bronze does not reconcile with Silver + Quarantine"
)

# COMMAND ----------

latest_coordinator = (
    spark.table("agent_coordinator_log")
    .orderBy(F.col("executed_at").desc())
    .limit(1)
)

display(latest_coordinator)

# COMMAND ----------

gold_tables = [
    "gold_daily_booking_performance",
    "gold_cancellation_analysis",
    "gold_value_by_vehicle_payment",
    "gold_peak_hour_demand",
    "gold_top_pickup_locations",
    "gold_top_drop_locations",
    "gold_vehicle_performance"
]

for table_name in gold_tables:
    if spark.catalog.tableExists(table_name):
        count = spark.table(table_name).count()
        print(f"{table_name}: {count} rows")
        assert count > 0, f"{table_name} is empty"
    else:
        raise ValueError(f"Missing Gold table: {table_name}")