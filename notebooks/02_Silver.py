# Databricks notebook source
# spark.conf.set(
#     "fs.azure.account.key.streamingdatalake2026.dfs.core.windows.net",
#     "<YOUR_STORAGE_ACCOUNT_KEY>"
# )

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    length,
    current_date,
    to_date
)
from delta import configure_spark_with_delta_pip

# ==========================================================
# Spark Session
# ==========================================================

builder = (
    SparkSession.builder
    .appName("StreamingPlatformSilverLayer")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ==========================================================
# Azure Paths
# ==========================================================

BRONZE_DIR = "abfss://datalake@streamingdatalake2026.dfs.core.windows.net/bronze"
SILVER_DIR = "abfss://datalake@streamingdatalake2026.dfs.core.windows.net/silver"

# ==========================================================
# Users
# ==========================================================

def process_users():

    print("\n" + "=" * 70)
    print("Processing Users")
    print("=" * 70)

    df = spark.read.format("delta").load(f"{BRONZE_DIR}/users")

    print(f"Bronze Users : {df.count():,}")

    df = (
        df.dropDuplicates(["user_id"])
          .filter(col("user_id").isNotNull())
          .withColumn(
              "registration_date",
              to_date(col("registration_date"))
          )
          .filter(col("registration_date") <= current_date())
          .filter(
              col("subscription_type").isin(
                  "Basic",
                  "Standard",
                  "Premium"
              )
          )
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_DIR}/users")
    )

    silver_df = spark.read.format("delta").load(f"{SILVER_DIR}/users")

    print(f"Silver Users : {silver_df.count():,}")

    # display(
    #     silver_df.orderBy(
    #         col("user_id").desc()
    #     ).limit(10)
    # )

# ==========================================================
# Movies
# ==========================================================

def process_movies():

    print("\n" + "=" * 70)
    print("Processing Movies")
    print("=" * 70)

    df = spark.read.format("delta").load(f"{BRONZE_DIR}/movies")

    print(f"Bronze Movies : {df.count():,}")

    df = (
        df.dropDuplicates(["movie_id"])
          .filter(col("movie_id").isNotNull())
          .filter(length(col("title")) > 0)
          .filter(col("duration_minutes") > 0)
          .filter(col("release_year").between(1900, 2030))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_DIR}/movies")
    )

    silver_df = spark.read.format("delta").load(f"{SILVER_DIR}/movies")

    print(f"Silver Movies : {silver_df.count():,}")

    # display(
    #     silver_df.orderBy(
    #         col("movie_id").desc()
    #     ).limit(10)
    # )

# ==========================================================
# Watch Logs
# ==========================================================

def process_watch_logs():

    print("\n" + "=" * 70)
    print("Processing Watch Logs")
    print("=" * 70)

    logs = spark.read.format("delta").load(f"{BRONZE_DIR}/watch_logs")
    movies = spark.read.format("delta").load(f"{BRONZE_DIR}/movies")

    print(f"Bronze Watch Logs : {logs.count():,}")

    df = (
        logs.join(
            movies.select(
                "movie_id",
                "duration_minutes"
            ),
            "movie_id",
            "left"
        )
        .dropDuplicates(["log_id"])
        .filter(col("log_id").isNotNull())
        .filter(col("minutes_watched") >= 0)
        .filter(
            col("minutes_watched") <=
            col("duration_minutes")
        )
        .withColumn(
            "watch_date",
            to_date(col("watch_date"))
        )
        .filter(col("watch_date") <= current_date())
        .drop("duration_minutes")
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_DIR}/watch_logs")
    )

    silver_df = spark.read.format("delta").load(f"{SILVER_DIR}/watch_logs")

    print(f"Silver Watch Logs : {silver_df.count():,}")

    # display(
    #     silver_df.orderBy(
    #         col("log_id").desc()
    #     ).limit(10)
    # )

# ==========================================================
# Main
# ==========================================================

if __name__ == "__main__":

    process_users()
    process_movies()
    process_watch_logs()

    print("\n" + "=" * 70)
    print("Silver Layer Processing Completed Successfully")
    print("=" * 70)

    spark.stop()