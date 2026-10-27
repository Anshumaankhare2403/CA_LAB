# Databricks notebook source
# MAGIC %md
# MAGIC # Experiment 05: Real-Time Streaming with Azure Event Hub & Databricks
# MAGIC **Course**: Cloud Analytics (25MCAR0141) | **CO**: CO3
# MAGIC
# MAGIC This notebook reads real-time streaming events from Azure Event Hub using
# MAGIC PySpark Structured Streaming, parses JSON payloads, applies tumbling window
# MAGIC aggregations, and displays live results.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Event Hub Connector
# MAGIC
# MAGIC Before running, install the Maven library on your cluster:
# MAGIC ```
# MAGIC com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
# MAGIC ```
# MAGIC Go to: Cluster → Libraries → Install New → Maven → Search Packages

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Event Hub Connection

# COMMAND ----------

# Event Hub connection configuration
EVENT_HUB_CONNECTION_STRING = (
    "Endpoint=sb://calab-eventhub-ns-141.servicebus.windows.net/;"
    "SharedAccessKeyName=DatabricksPolicy;"
    "SharedAccessKey=<YOUR_SHARED_ACCESS_KEY>;"
    "EntityPath=calab-eventhub-141"
)

# Event Hub Spark connector config
ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
        EVENT_HUB_CONNECTION_STRING
    ),
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
    "maxEventsPerTrigger": 100,
}

print("Event Hub configuration set successfully.")
print(f"Namespace: calab-eventhub-ns-141")
print(f"Event Hub: calab-eventhub-141")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read Streaming Data from Event Hub

# COMMAND ----------

# Read stream from Event Hub
raw_stream = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

print("Streaming DataFrame schema from Event Hub:")
raw_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parse JSON Event Payload

# COMMAND ----------

from pyspark.sql.functions import (
    col, from_json, window, count, avg, max as spark_max,
    current_timestamp, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

# Define the JSON schema matching producer.py output
event_schema = StructType([
    StructField("student_name", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("course", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device", StringType(), True),
])

# Parse the body (binary → string → JSON)
parsed_stream = (
    raw_stream
    .select(
        col("enqueuedTime").alias("enqueued_time"),
        col("offset"),
        col("sequenceNumber").alias("sequence_number"),
        from_json(col("body").cast("string"), event_schema).alias("event")
    )
    .select(
        "enqueued_time",
        "offset",
        "sequence_number",
        col("event.student_name").alias("student_name"),
        col("event.event_type").alias("event_type"),
        col("event.course").alias("course"),
        col("event.score").alias("score"),
        col("event.timestamp").alias("event_timestamp"),
        col("event.session_id").alias("session_id"),
        col("event.device").alias("device"),
    )
)

print("Parsed stream schema:")
parsed_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Display Raw Streaming Events

# COMMAND ----------

# Display real-time events as they arrive (Databricks auto-updates)
display(parsed_stream)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tumbling Window Aggregation — Events Per Student (5-min windows)

# COMMAND ----------

# Add watermark for late data handling and apply tumbling window
windowed_counts = (
    parsed_stream
    .withWatermark("enqueued_time", "2 minutes")
    .groupBy(
        window(col("enqueued_time"), "5 minutes"),
        col("student_name")
    )
    .agg(
        count("*").alias("event_count"),
        avg("score").alias("avg_score"),
        spark_max("score").alias("max_score")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "student_name",
        "event_count",
        "avg_score",
        "max_score"
    )
)

display(windowed_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Event Type Distribution (Streaming)

# COMMAND ----------

# Aggregate event types across all students
event_type_agg = (
    parsed_stream
    .withWatermark("enqueued_time", "2 minutes")
    .groupBy(
        window(col("enqueued_time"), "5 minutes"),
        col("event_type")
    )
    .agg(count("*").alias("total_events"))
    .select(
        col("window.start").alias("window_start"),
        "event_type",
        "total_events"
    )
)

display(event_type_agg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Streaming Output to ADLS Gen2

# COMMAND ----------

# Write parsed events to ADLS Gen2 in append mode (Parquet)
output_query = (
    parsed_stream
    .writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", "/mnt/output/streaming_checkpoint")
    .option("path", "/mnt/output/streaming_events")
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"Streaming query started: {output_query.id}")
print(f"Status: {output_query.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Monitor & Stop Stream

# COMMAND ----------

# Check active streaming queries
for q in spark.streams.active:
    print(f"Query: {q.name} | ID: {q.id} | Status: {q.status}")

# COMMAND ----------

# Stop the streaming query when done (uncomment to stop)
# output_query.stop()
# print("Streaming query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Experiment 05 Complete** — Real-time streaming data was successfully processed
# MAGIC using Azure Databricks Structured Streaming and Azure Event Hub.
