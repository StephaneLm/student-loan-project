"""Structured Streaming job that reads CSV lines from Kafka, parses them and stores inside Delta Lake."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, col, current_timestamp
from delta import configure_spark_with_delta_pip

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "student_loans_raw")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/data/streaming/chkpt")
DEST_PATH = os.getenv("DEST_PATH", "/data/processed/student_loans")

spark = configure_spark_with_delta_pip(SparkSession.builder) \
    .appName("StudentLoanStreaming") \
    .getOrCreate()

schema = "`Loan_ID` string, `Disbursement_Amount` double, `Loan_Date` date, `Borrower_State` string"

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as csv")

parsed = df.select(from_csv(col("csv"), schema, {"header": "false"}).alias("data")).select("data.*") \
           .withColumn("ingest_ts", current_timestamp())

query = parsed.writeStream \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT) \
    .outputMode("append") \
    .start(DEST_PATH)

query.awaitTermination()
