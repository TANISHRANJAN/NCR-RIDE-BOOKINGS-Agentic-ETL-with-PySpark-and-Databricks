# Databricks notebook source
from pyspark.sql import functions as F

tables_to_check = [
    "bronze_ncr_ride_bookings",
    "silver_ncr_ride_bookings",
    "quarantine_ncr_ride_bookings",
    "gold_daily_booking_performance",
    "gold_cancellation_analysis",
    "gold_value_by_vehicle_payment",
    "gold_peak_hour_demand",
    "gold_top_pickup_locations",
    "gold_top_drop_locations",
    "gold_vehicle_performance"
]

table_results = []

for table_name in tables_to_check:
    if spark.catalog.tableExists(table_name):
        row_count = spark.table(table_name).count()
        table_results.append((table_name, row_count, "AVAILABLE"))
    else:
        table_results.append((table_name, 0, "MISSING"))

table_status_df = spark.createDataFrame(
    table_results,
    ["table_name", "row_count", "status"]
)

display(table_status_df)

# COMMAND ----------

agent_tables = [
    "data_quality_agent_log",
    "supervisor_agent_log",
    "etl_agent_log",
    "analytics_agent_results",
    "explanation_agent_results",
    "agent_coordinator_log"
]

for table_name in agent_tables:
    print(f"\n--- {table_name} ---")

    if spark.catalog.tableExists(table_name):
        display(
            spark.table(table_name)
            .orderBy(F.col("executed_at").desc())
            if "executed_at" in spark.table(table_name).columns
            else spark.table(table_name)
        )
    else:
        print("Table not found")

# COMMAND ----------


from datetime import datetime, timezone

run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

# Create the Markdown table rows
table_lines = [
    f"| {row['table_name']} | {row['row_count']} | {row['status']} |"
    for row in table_status_df.collect()
]

# FIX: table_results contains tuples:
# (table_name, row_count, status)
all_tables_available = all(
    row[2] == "AVAILABLE"
    for row in table_results
)

report_status = (
    "TABLE VALIDATION PASSED"
    if all_tables_available
    else "TABLE VALIDATION NEEDS ATTENTION"
)

print("Report status:", report_status)
print("Tables checked:", len(table_results))
print("All tables available:", all_tables_available)


# COMMAND ----------


report = f"""
# NCR Ride Bookings — Agentic ETL Project Report

## 1. Project Objective
Build a reliable ETL pipeline using PySpark and Databricks
to process NCR ride-booking data, perform data-quality checks,
quarantine invalid records, and generate analytical Gold tables.

## 2. Architecture
CSV Source
    -> Bronze Layer
    -> Silver Layer
    -> Quarantine Layer for rejected records
    -> Gold Layer for analytics
    -> Rule-based agents and project report

## 3. Data Processing Summary
- Bronze records: 150,000
- Silver records: 148,767
- Quarantined records: 1,233
- Reconciliation: Silver + Quarantine = Bronze

## 4. Gold Tables
- gold_daily_booking_performance
- gold_cancellation_analysis
- gold_value_by_vehicle_payment
- gold_peak_hour_demand
- gold_top_pickup_locations
- gold_top_drop_locations
- gold_vehicle_performance

## 5. Orchestration
The Databricks Job contains two dependent notebook tasks:
1. bronze_silver_processing
2. gold_and_agent_processing

## 6. Agent Design
The project uses deterministic, rule-based logic for data-quality
decisions, ETL status, analytics, explanations, and coordination.

## 7. Testing
The pipeline and table validations have been executed.
An isolated invalid-rating test checks whether ratings outside
the permitted 1–5 range are detected.

## 8. Limitations
Microsoft Foundry integration was not implemented.
The project therefore uses rule-based logic rather than claiming
LLM-powered agent functionality. Additional failure scenarios
and automatic recovery require further testing.

## 9. Future Improvements
- Add missing-source and schema-change tests.
- Add duplicate and null-spike fault tests.
- Implement automatic stopping for critical quality failures.
- Add alerts, retries, and monitoring.
- Integrate Microsoft Foundry when the required environment is available.

## 10. Table Validation
Report status: {report_status}

{chr(10).join(table_lines)}
"""

print("Report created successfully.")
print("Report status:", report_status)
print(report)


# COMMAND ----------

report_df = spark.createDataFrame(
    [(run_time, report_status, report)],
    ["generated_at", "report_status", "report_markdown"]
)

report_df.write.format("delta").mode("overwrite").saveAsTable(
    "ncr_ride_bookings_project_report"
)

display(spark.table("ncr_ride_bookings_project_report"))