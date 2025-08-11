# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

csv_schema = StructType([
    StructField("user_id",IntegerType(),True),
    StructField("event_type",StringType(),True),
    StructField("value",DoubleType(),True),
    StructField("event_time",StringType(),True)])
raw_stream = (spark.readStream.format("csv")
              .schema(csv_schema)
              .option("header","true")
              .option("maxFilesPerTrigger", 1)
              .load("/mnt/inputdata/streams/csv_input/"))
display(raw_stream)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, current_timestamp

parsed_stream = (
    raw_stream
    .withColumn("event_time_parsed", to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssZ"))
    .drop("event_time")
    .withColumnRenamed("event_time_parsed", "event_time")
    .withColumn("Ingest_time", current_timestamp())
)

display(parsed_stream)


# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists raw_events_mang1(
# MAGIC   user_id INT,
# MAGIC   event_type STRING,
# MAGIC   value DOUBLE,
# MAGIC   event_time TIMESTAMP,
# MAGIC   ingest_time TIMESTAMP
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

delta_writer = (
    parsed_stream.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/inputdata/streams/checkpoints/raw_events_mang1")
    .toTable("raw_events_mang1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET trigger = processingTime = '1 minute'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_events_mang1;

# COMMAND ----------

