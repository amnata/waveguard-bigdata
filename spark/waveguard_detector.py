from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as spark_sum,
    current_timestamp, lit, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType
)

spark = SparkSession.builder \
    .appName("WaveGuard_FraudDetector") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("sender_id", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("amount_fcfa", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("is_flagged", BooleanType(), True),
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "waveguard-kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df = parsed_df.withWatermark("timestamp", "10 seconds")

# RÈGLE 1 : VÉLOCITÉ
velocity_fraud = df \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("sender_id")
    ) \
    .agg(count("*").alias("tx_count")) \
    .filter(col("tx_count") > 5) \
    .select(
        col("sender_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("tx_count"),
        lit("VELOCITY_FRAUD").alias("fraud_type"),
        current_timestamp().alias("detected_at")
    )

# RÈGLE 2 : VOLUME
volume_fraud = df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "2 minutes"),
        col("sender_id")
    ) \
    .agg(spark_sum("amount_fcfa").alias("total_amount")) \
    .filter(col("total_amount") > 500000) \
    .select(
        col("sender_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_amount"),
        lit("VOLUME_FRAUD").alias("fraud_type"),
        current_timestamp().alias("detected_at")
    )

# SINK KAFKA — alertes
def write_to_kafka(df, label):
    return df.select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "waveguard-kafka:9092") \
        .option("topic", "fraud-alerts") \
        .option("checkpointLocation", f"/tmp/checkpoint_{label}") \
        .outputMode("update") \
        .start()

# SINK DATALAKE — transactions brutes en parquet (append fonctionne sur df non agrégé)
def write_to_datalake(df, label):
    return df.writeStream \
        .format("parquet") \
        .option("path", f"/tmp/waveguard_lake/{label}") \
        .option("checkpointLocation", f"/tmp/checkpoint_lake_{label}") \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()

# LANCEMENT
q1 = write_to_kafka(velocity_fraud, "velocity")
q2 = write_to_kafka(volume_fraud, "volume")

# Datalake : on écrit les transactions parsées brutes (append OK sur stream non agrégé)
l1 = write_to_datalake(parsed_df, "transactions")

spark.streams.awaitAnyTermination()
