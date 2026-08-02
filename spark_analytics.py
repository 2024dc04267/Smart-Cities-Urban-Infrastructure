import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, sum, avg, max, date_format, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
# Tell PySpark to download and include the Kafka package automatically
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 pyspark-shell'
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC = "urbanpulse.smart_meters"
OUTPUT_TOPIC = "ward_energy_summary"
STORAGE_BASE_PATH = "/home/hppa/Documents/urbanpulse_storage/ward_analytics"
CHECKPOINT_PATH = "/tmp/spark_checkpoints/ward_energy"

def main():
    print("Initializing Spark Session for UrbanPulse Ward Analytics...")
    
    spark = SparkSession.builder \
        .appName("UrbanPulse-WardEnergyAnalytics") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("ward_id", StringType(), True),
        StructField("kwh_reading", DoubleType(), True),
        StructField("voltage", IntegerType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = raw_df \
        .select(from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("timestamp").cast(TimestampType()))

    aggregated_df = parsed_df \
        .withWatermark("event_time", "45 minutes") \
        .groupBy(
            window(col("event_time"), "15 minutes"),
            col("ward_id")
        ) \
        .agg(
            sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            max("voltage").alias("peak_voltage")
        ) \
        .select(
            col("ward_id"),
            date_format(col("window.start"), "yyyy-MM-dd").alias("date"),
            col("window.start").cast("string").alias("window_start"),
            col("window.end").cast("string").alias("window_end"),
            col("total_kwh_consumed"),
            col("avg_power_factor"),
            col("peak_voltage")
        )

    def write_dual_sinks(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        # Sink 1: Write Parquet Dataset
        batch_df.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("ward_id", "date") \
            .save(STORAGE_BASE_PATH)

        # Sink 2: Write Kafka Output
        kafka_payload = batch_df.select(
            col("ward_id").alias("key"),
            to_json(struct("*")).alias("value")
        )
        kafka_payload.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("topic", OUTPUT_TOPIC) \
            .save()
    
    query = aggregated_df.writeStream \
    .trigger(processingTime='10 seconds') \
    .foreachBatch(write_dual_sinks) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("update") \
    .start()
    # query = aggregated_df.writeStream \
    #     .foreachBatch(write_dual_sinks) \
    #     .option("checkpointLocation", CHECKPOINT_PATH) \
    #     .outputMode("update") \
    #     .start()

    print(f"Analytics Pipeline active. Streaming to Kafka ({OUTPUT_TOPIC}) & Parquet ({STORAGE_BASE_PATH})...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
