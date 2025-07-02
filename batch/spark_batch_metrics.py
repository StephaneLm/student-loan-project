"""Batch job that computes loan KPIs and writes them to Parquet."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum as _sum, count, col

SOURCE = os.getenv("SOURCE_PATH", "/data/processed/student_loans")
OUT_PATH = os.getenv("OUT_PATH", "/data/curated/kpis")

spark = SparkSession.builder.appName("StudentLoanBatchMetrics").getOrCreate()

df = spark.read.format("delta").load(SOURCE)

kpis = (df
        .groupBy(year(col("Loan_Date")).alias("loan_year"))
        .agg(_sum("Disbursement_Amount").alias("total_amount"),
             count("Loan_ID").alias("loan_count"))
        .orderBy("loan_year"))

kpis.write.mode("overwrite").parquet(OUT_PATH)

print("KPIs written to", OUT_PATH)
