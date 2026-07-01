# Databricks notebook source
# spark.conf.set(
#     "fs.azure.account.key.streamingdatalake2026.dfs.core.windows.net",
#     "<YOUR_STORAGE_ACCOUNT_KEY>"
# )

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import random
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()

# =====================================================
# CONFIGURATION (ADLS PATHS)
# =====================================================

BASE_PATH = "abfss://datalake@streamingdatalake2026.dfs.core.windows.net"

USERS_PATH = f"{BASE_PATH}/raw/users"
MOVIES_PATH = f"{BASE_PATH}/raw/movies"
LOGS_PATH = f"{BASE_PATH}/raw/watch_logs"

# =====================================================
# PARAMETERS
# =====================================================

INITIAL_USERS = 100000
INITIAL_MOVIES = 10000
INITIAL_LOGS = 1000000

NEW_USERS_PER_RUN = 1000
NEW_MOVIES_PER_RUN = 200
NEW_LOGS_PER_RUN = 50000

# =====================================================
# USERS GENERATION
# =====================================================

try:
    users_df = spark.read.parquet(USERS_PATH)
    existing_users = users_df.count()
    start_user = existing_users + 1

    new_users = [
        (
            i,
            random.choice(["Basic", "Standard", "Premium"]),
            datetime.now()
        )
        for i in range(start_user, start_user + NEW_USERS_PER_RUN)
    ]

    users_schema = ["user_id", "subscription_type", "registration_date"]
    new_users_df = spark.createDataFrame(new_users, users_schema)

    new_users_df.write.mode("append").parquet(USERS_PATH)

except:
    # First run
    users = [
        (
            i,
            random.choice(["Basic", "Standard", "Premium"]),
            datetime.now() - timedelta(days=random.randint(1, 730))
        )
        for i in range(1, INITIAL_USERS + 1)
    ]

    users_df = spark.createDataFrame(users, ["user_id", "subscription_type", "registration_date"])
    users_df.write.mode("overwrite").parquet(USERS_PATH)

# =====================================================
# MOVIES GENERATION
# =====================================================

genres = [
    "Action","Comedy","Drama","Sci-Fi","Thriller","Romance",
    "Documentary","Animation","Fantasy","Crime","Adventure","Mystery","Horror"
]

try:
    movies_df = spark.read.parquet(MOVIES_PATH)
    existing_movies = movies_df.count()
    start_movie = existing_movies + 1

    new_movies = [
        (
            i,
            f"Movie_{i}",
            random.choice(genres),
            random.randint(2023, 2026),
            random.randint(80, 180)
        )
        for i in range(start_movie, start_movie + NEW_MOVIES_PER_RUN)
    ]

    movies_schema = ["movie_id", "title", "genre", "release_year", "duration_minutes"]
    new_movies_df = spark.createDataFrame(new_movies, movies_schema)

    new_movies_df.write.mode("append").parquet(MOVIES_PATH)

except:
    movies = [
        (
            i,
            f"Movie_{i}",
            random.choice(genres),
            random.randint(2000, 2026),
            random.randint(80, 180)
        )
        for i in range(1, INITIAL_MOVIES + 1)
    ]

    movies_df = spark.createDataFrame(
        movies,
        ["movie_id", "title", "genre", "release_year", "duration_minutes"]
    )

    movies_df.write.mode("overwrite").parquet(MOVIES_PATH)

# =====================================================
# WATCH LOGS GENERATION
# =====================================================

users_df = spark.read.parquet(USERS_PATH)
movies_df = spark.read.parquet(MOVIES_PATH)

user_ids = [row.user_id for row in users_df.select("user_id").collect()]
movie_data = movies_df.select("movie_id", "duration_minutes").collect()

try:
    logs_df = spark.read.parquet(LOGS_PATH)
    existing_logs = logs_df.count()
    start_log = existing_logs + 1
    num_logs = NEW_LOGS_PER_RUN

    initial_load = False

except:
    start_log = 1
    num_logs = INITIAL_LOGS
    initial_load = True

logs = []

for i in range(num_logs):

    movie = random.choice(movie_data)
    duration = movie["duration_minutes"]

    logs.append((
        start_log + i,
        random.choice(user_ids),
        movie["movie_id"],
        datetime.now() - timedelta(days=random.randint(0, 30)) if initial_load else datetime.now(),
        random.randint(max(5, int(duration * 0.1)), duration)
    ))

logs_schema = [
    "log_id",
    "user_id",
    "movie_id",
    "watch_date",
    "minutes_watched"
]

logs_df = spark.createDataFrame(logs, logs_schema)

logs_df.write.mode("append").parquet(LOGS_PATH)

# =====================================================
# SUMMARY OUTPUT
# =====================================================

print("===================================")
print("DATA GENERATION COMPLETED")
print("===================================")

print("Users Path:", USERS_PATH)
print("Movies Path:", MOVIES_PATH)
print("Logs Path:", LOGS_PATH)

print("New records generated successfully!")
print("===================================")

# Write a clean, brand new trigger file to signal ADF that we are done
#dbutils.fs.put("abfss://datalake@streamingdatalake2026.dfs.core.windows.net/raw/trigger.txt", "done", True)