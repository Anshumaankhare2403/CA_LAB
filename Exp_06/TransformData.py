# Databricks notebook source
# MAGIC %md
# MAGIC # Experiment 06: TransformData — ADF-Triggered Databricks Notebook
# MAGIC **Course**: Cloud Analytics (25MCAR0141) | **CO**: CO3
# MAGIC
# MAGIC This notebook is triggered by Azure Data Factory as part of an end-to-end
# MAGIC pipeline. It reads raw CSV data from ADLS Gen2, performs data cleaning,
# MAGIC feature engineering, and writes transformed output in Parquet format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read Raw Data from ADLS Gen2

# COMMAND ----------

# Read raw student performance CSV from mounted ADLS Gen2 storage
# Path corresponds to calablake141/rawdata container
raw_df = spark.read.csv(
    "/mnt/rawdata/student_performance.csv",
    header=True,
    inferSchema=True
)

print(f"Raw dataset loaded: {raw_df.count()} rows, {len(raw_df.columns)} columns")
raw_df.printSchema()
display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Profiling — Schema & Null Analysis

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when, count, lit

# Count nulls per column
null_report = raw_df.select(
    [spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in raw_df.columns]
)
print("=== Null Counts ===")
display(null_report)

# Summary statistics
print("=== Summary Statistics ===")
display(raw_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Cleaning

# COMMAND ----------

from pyspark.sql.functions import mean, round as spark_round, trim, upper, lower

# --- 3a. Standardize column names (lowercase, strip whitespace) ---
for old_name in raw_df.columns:
    raw_df = raw_df.withColumnRenamed(old_name, old_name.strip().replace(" ", "_"))

# --- 3b. Remove exact duplicate rows ---
before_dedup = raw_df.count()
cleaned_df = raw_df.dropDuplicates()
after_dedup = cleaned_df.count()
print(f"Duplicates removed: {before_dedup - after_dedup}")

# --- 3c. Drop rows with null in critical columns ---
critical_cols = ["Name", "MathScore", "ReadingScore", "WritingScore", "FinalGrade"]
cleaned_df = cleaned_df.dropna(subset=critical_cols)
print(f"Rows after dropping critical nulls: {cleaned_df.count()}")

# --- 3d. Fill missing numeric values with column median ---
from pyspark.ml.feature import Imputer

numeric_fill_cols = ["Age", "AttendanceRate", "StudyHoursPerWeek"]
imputer = Imputer(
    inputCols=numeric_fill_cols,
    outputCols=numeric_fill_cols,  # overwrite in-place
    strategy="median"
)
cleaned_df = imputer.fit(cleaned_df).transform(cleaned_df)

# --- 3e. Enforce data types ---
cleaned_df = (
    cleaned_df
    .withColumn("StudentID", col("StudentID").cast("int"))
    .withColumn("Age", col("Age").cast("int"))
    .withColumn("MathScore", col("MathScore").cast("int"))
    .withColumn("ReadingScore", col("ReadingScore").cast("int"))
    .withColumn("WritingScore", col("WritingScore").cast("int"))
    .withColumn("AttendanceRate", spark_round(col("AttendanceRate"), 1))
    .withColumn("StudyHoursPerWeek", spark_round(col("StudyHoursPerWeek"), 1))
)

print(f"Cleaned dataset: {cleaned_df.count()} rows")
display(cleaned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Feature Engineering

# COMMAND ----------

from pyspark.sql.functions import (
    when, round as spark_round, concat_ws
)

# --- 4a. Performance Score (weighted average) ---
# Math: 40%, Reading: 30%, Writing: 30%
transformed_df = cleaned_df.withColumn(
    "PerformanceScore",
    spark_round(
        col("MathScore") * 0.4 + col("ReadingScore") * 0.3 + col("WritingScore") * 0.3,
        2
    )
)

# --- 4b. Grade Category (simplified) ---
transformed_df = transformed_df.withColumn(
    "GradeCategory",
    when(col("FinalGrade").isin("A"), "Excellent")
    .when(col("FinalGrade").isin("B"), "Good")
    .when(col("FinalGrade").isin("C"), "Average")
    .when(col("FinalGrade").isin("D"), "Below Average")
    .otherwise("Failing")
)

# --- 4c. Attendance Status ---
transformed_df = transformed_df.withColumn(
    "AttendanceStatus",
    when(col("AttendanceRate") >= 90.0, "Excellent")
    .when(col("AttendanceRate") >= 75.0, "Good")
    .when(col("AttendanceRate") >= 60.0, "At Risk")
    .otherwise("Critical")
)

# --- 4d. Study Intensity ---
transformed_df = transformed_df.withColumn(
    "StudyIntensity",
    when(col("StudyHoursPerWeek") >= 15, "High")
    .when(col("StudyHoursPerWeek") >= 8, "Medium")
    .otherwise("Low")
)

print("=== Transformed Dataset with Engineered Features ===")
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary Statistics

# COMMAND ----------

from pyspark.sql.functions import avg, min as spark_min, max as spark_max, stddev

# Summary by GradeCategory
summary_df = (
    transformed_df
    .groupBy("GradeCategory")
    .agg(
        count("*").alias("StudentCount"),
        spark_round(avg("PerformanceScore"), 2).alias("AvgPerformance"),
        spark_round(avg("AttendanceRate"), 2).alias("AvgAttendance"),
        spark_round(avg("StudyHoursPerWeek"), 1).alias("AvgStudyHours"),
        spark_min("PerformanceScore").alias("MinPerformance"),
        spark_max("PerformanceScore").alias("MaxPerformance"),
    )
    .orderBy("GradeCategory")
)

print("=== Summary by Grade Category ===")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Transformed Output to ADLS Gen2

# COMMAND ----------

# Write cleaned and transformed data as Parquet
output_path = "/mnt/output/student_performance_cleaned"
transformed_df.write.mode("overwrite").parquet(output_path)
print(f"Transformed data written to: {output_path}")

# Write summary statistics as CSV
summary_path = "/mnt/output/summary_stats"
summary_df.write.mode("overwrite").option("header", True).csv(summary_path)
print(f"Summary statistics written to: {summary_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verification — Read Back Output

# COMMAND ----------

# Verify Parquet output
verify_df = spark.read.parquet(output_path)
print(f"Verified output: {verify_df.count()} rows, {len(verify_df.columns)} columns")
display(verify_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **TransformData Notebook Complete** — Raw data was cleaned, features engineered,
# MAGIC and transformed output written to ADLS Gen2 as Parquet. This notebook is
# MAGIC orchestrated by Azure Data Factory pipeline `PL_ADF_Databricks`.
