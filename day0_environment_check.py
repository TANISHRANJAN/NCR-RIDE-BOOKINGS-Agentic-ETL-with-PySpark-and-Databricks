# Databricks notebook source

# Day 0: Check the Databricks environment

print("Capstone 3 - Uber Taxi Agentic ETL")
print("=" * 40)

print("Spark version:", spark.version)
print("Python is working correctly!")

# Check that Spark can perform a simple calculation
result = spark.range(1, 6)

display(result)


# COMMAND ----------

