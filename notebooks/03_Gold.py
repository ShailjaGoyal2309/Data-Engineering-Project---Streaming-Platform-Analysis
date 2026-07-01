# Databricks notebook source
# spark.conf.set(
#     "fs.azure.account.key.streamingdatalake2026.dfs.core.windows.net",
#     "<YOUR_STORAGE_ACCOUNT_KEY>"
# )

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round,
    sum as spark_sum,
    count,
    when,
    avg
)

# ==========================================================
# Spark Session
# ==========================================================

spark = (
    SparkSession.builder
    .appName("StreamingPlatformGoldLayer")
    .getOrCreate()
)

# ==========================================================
# Azure Data Lake Paths
# ==========================================================

SILVER_DATA_DIR = "abfss://datalake@streamingdatalake2026.dfs.core.windows.net/silver"
GOLD_DATA_DIR = "abfss://datalake@streamingdatalake2026.dfs.core.windows.net/gold_parquet"

# ==========================================================
# Gold Layer
# ==========================================================

def process_gold_layer():

    try:

        print("\n" + "=" * 70)
        print("Loading Silver Tables")
        print("=" * 70)

        users_df = spark.read.format("delta").load(f"{SILVER_DATA_DIR}/users")
        movies_df = spark.read.format("delta").load(f"{SILVER_DATA_DIR}/movies")
        logs_df = spark.read.format("delta").load(f"{SILVER_DATA_DIR}/watch_logs")

        # ======================================================
        # Watch Completion Fact
        # ======================================================

        watch_completion_df = logs_df.join(
            movies_df,
            "movie_id",
            "inner"
        )

        watch_completion_df = watch_completion_df.withColumn(
            "completion_percentage",
            round(
                (col("minutes_watched") / col("duration_minutes")) * 100,
                2
            )
        )

        watch_completion_df = watch_completion_df.withColumn(
            "completion_percentage",
            when(
                col("completion_percentage") > 100,
                100
            ).otherwise(col("completion_percentage"))
        )

        (
            watch_completion_df.write
            .mode("overwrite")
            .parquet(f"{GOLD_DATA_DIR}/watch_completion_fact")
        )

        print("watch_completion_fact created.")

        # ======================================================
        # User Genre Preference Matrix
        # ======================================================

        user_genre_pref_df = (
            watch_completion_df
            .groupBy("user_id", "genre")
            .agg(
                spark_sum("minutes_watched").alias("total_minutes_watched"),
                count("log_id").alias("movies_watched")
            )
        )

        preference_matrix_df = (
            user_genre_pref_df
            .groupBy("user_id")
            .pivot("genre")
            .sum("total_minutes_watched")
            .fillna(0)
        )

        (
            preference_matrix_df.write
            .mode("overwrite")
            .parquet(f"{GOLD_DATA_DIR}/user_genre_preference_matrix")
        )

        print("user_genre_preference_matrix created.")

        # ======================================================
        # Trending Genres
        # ======================================================

        trending_genres_df = (
            user_genre_pref_df
            .groupBy("genre")
            .agg(
                spark_sum("total_minutes_watched").alias("total_platform_minutes")
            )
            .orderBy(col("total_platform_minutes").desc())
        )

        (
            trending_genres_df.write
            .mode("overwrite")
            .parquet(f"{GOLD_DATA_DIR}/trending_genres")
        )

        print("trending_genres created.")

        # ======================================================
        # Movie Popularity
        # ======================================================

        movie_popularity_df = (
            watch_completion_df
            .groupBy("movie_id", "title", "genre")
            .agg(
                count("log_id").alias("total_views"),
                spark_sum("minutes_watched").alias("watch_time"),
                round(
                    avg("completion_percentage"),
                    2
                ).alias("avg_completion")
            )
            .orderBy(col("total_views").desc())
        )

        (
            movie_popularity_df.write
            .mode("overwrite")
            .parquet(f"{GOLD_DATA_DIR}/movie_popularity")
        )

        print("movie_popularity created.")

        # ======================================================
        # Subscription Summary
        # ======================================================

        subscription_summary_df = (
            users_df
            .groupBy("subscription_type")
            .agg(
                count("*").alias("total_users")
            )
        )

        (
            subscription_summary_df.write
            .mode("overwrite")
            .parquet(f"{GOLD_DATA_DIR}/subscription_summary")
        )

        print("subscription_summary created.")

        print("\n" + "=" * 70)
        print("Gold Layer Processing Completed Successfully")
        print("=" * 70)

    except Exception as e:
        print("Gold Layer Processing Failed")
        print(e)

# ==========================================================
# Main
# ==========================================================

if __name__ == "__main__":

    process_gold_layer()

    spark.stop()