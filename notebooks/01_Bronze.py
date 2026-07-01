# Databricks notebook source
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# ==========================================================
# Azure Storage Authentication
# ==========================================================

# spark.conf.set(
#     "fs.azure.account.key.streamingdatalake2026.dfs.core.windows.net",
#     "<YOUR_STORAGE_ACCOUNT_KEY>"
# )

# ==========================================================
# Spark Session
# ==========================================================

builder = (
    SparkSession.builder
    .appName("StreamingPlatformBronzeLayer")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = builder.getOrCreate()

# ==========================================================
# Azure Data Lake Paths
# ==========================================================

RAW_DATA_DIR = "abfss://datalake@streamingdatalake2026.dfs.core.windows.net/raw"
BRONZE_DATA_DIR = "abfss://datalake@streamingdatalake2026.dfs.core.windows.net/bronze"

# ==========================================================
# Bronze Ingestion Function
# ==========================================================

def ingest_to_bronze(table_name):

    print("\n" + "=" * 70)
    print(f"Processing Table : {table_name}")
    print("=" * 70)

    source_path = f"{RAW_DATA_DIR}/{table_name}"
    destination_path = f"{BRONZE_DATA_DIR}/{table_name}"

    try:

        # ======================================================
        # Read latest Parquet data from Raw
        # ======================================================

        df = (
            spark.read
            .format("parquet")
            .load(source_path)
        )

        total_records = df.count()

        print(f"Records Read : {total_records:,}")

        # ======================================================
        # Write Bronze Delta
        # ======================================================

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(destination_path)
        )

        print("Bronze Delta Table Written Successfully")

        # ======================================================
        # Verification
        # ======================================================

        bronze_df = (
            spark.read
            .format("delta")
            .load(destination_path)
        )

        print(f"Total Records in Bronze : {bronze_df.count():,}")

        print("\nLatest Records")

        # if table_name == "users":
        #     display(bronze_df.orderBy("user_id", ascending=False).limit(10))

        # elif table_name == "movies":
        #     display(bronze_df.orderBy("movie_id", ascending=False).limit(10))

        # else:
        #     display(bronze_df.orderBy("log_id", ascending=False).limit(10))

    except Exception as e:

        print(f"Error processing {table_name}")
        print(e)

# ==========================================================
# Main
# ==========================================================

if __name__ == "__main__":

    tables = [
        "users",
        "movies",
        "watch_logs"
    ]

    for table in tables:
        ingest_to_bronze(table)

    print("\n" + "=" * 70)
    print("Bronze Layer Processing Completed Successfully")
    print("=" * 70)

    spark.stop()