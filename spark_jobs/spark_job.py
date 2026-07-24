"""
GitPulse — Spark batch processing layer
=========================================
Reads downloaded GHArchive .json.gz files, filters to the 7 target event
types and your curated ~500-repo list, engineers repo-level features per
time window, and writes:
  - Bronze layer (cleaned, filtered raw events)  -> MinIO (parquet)
  - Gold layer   (aggregated training rows)      -> PostgreSQL

Run with:
  spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
    spark_process_gharchive.py

Env vars expected (set these in your .env / docker-compose):
  MINIO_ENDPOINT     e.g. http://minio:9000
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  PG_HOST            e.g. postgres
  PG_PORT            5432
  PG_DB               gitpulse
  PG_USER
    password=os.environ.get("PG_PASSWORD", ""),
  GHARCHIVE_INPUT_PATH   local/mounted path to your downloaded .json.gz files
  CURATED_REPOS_PATH     path to a text file, one "owner/repo" per line
"""

import os

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# 1. Spark session — configured to talk to MinIO as if it were S3
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("GitPulse-BatchProcessing")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

TARGET_EVENT_TYPES = [
    "PushEvent", "WatchEvent", "ForkEvent", "IssuesEvent",
    "PullRequestEvent", "CreateEvent", "IssueCommentEvent",
]

INPUT_PATH = os.environ.get("GHARCHIVE_INPUT_PATH", "/data/gharchive/*.json.gz")
CURATED_REPOS_PATH = os.environ.get("CURATED_REPOS_PATH", "/data/curated_repos.txt")

# ---------------------------------------------------------------------------
# 2. Load curated repo allowlist (your ~500 repos across 5 categories)
# ---------------------------------------------------------------------------
with open(CURATED_REPOS_PATH) as f:
    curated_repos = [line.strip() for line in f if line.strip()]
curated_repos_df = spark.createDataFrame([(r,) for r in curated_repos], ["repo_name"])

print(f"[GitPulse] Loaded {len(curated_repos)} curated repos")

# ---------------------------------------------------------------------------
# 3. Read raw GHArchive JSON, filter to target event types + curated repos
# ---------------------------------------------------------------------------
raw = spark.read.json(INPUT_PATH)

# GHArchive schema: top-level "type", "repo.name", "created_at", "actor.login", "payload"
filtered = (
    raw.filter(F.col("type").isin(TARGET_EVENT_TYPES))
    .withColumn("repo_name", F.col("repo.name"))
    .join(F.broadcast(curated_repos_df), on="repo_name", how="inner")
    .withColumn("event_ts", F.to_timestamp("created_at"))
    .withColumn("event_date", F.to_date("event_ts"))
)

bronze = filtered.select(
    "repo_name", "type", "event_ts", "event_date",
    F.col("actor.login").alias("actor_login"),
    "payload",
)

bronze_count = bronze.count()
print(f"[GitPulse] Bronze layer row count after filtering: {bronze_count}")

# ---------------------------------------------------------------------------
# 4. Write bronze layer to MinIO (S3-compatible), partitioned by event type
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000").rstrip("/")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = "gitpulse-bronze"

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

try:
    s3_client.head_bucket(Bucket=BRONZE_BUCKET)
except ClientError as exc:
    error_code = exc.response.get("Error", {}).get("Code")
    if error_code in {"404", "NoSuchBucket"}:
        s3_client.create_bucket(Bucket=BRONZE_BUCKET)
    else:
        raise

BRONZE_PATH = "s3a://gitpulse-bronze/events/"
bronze.write.mode("overwrite").partitionBy("type").parquet(BRONZE_PATH)
print(f"[GitPulse] Bronze layer written to {BRONZE_PATH}")

# ---------------------------------------------------------------------------
# 5. Feature engineering — aggregate to weekly repo-level rows
#    (this is what becomes your ~15,000 training rows: ~500 repos x
#    ~26 weekly windows across 6 months)
# ---------------------------------------------------------------------------
weekly = bronze.withColumn("week_start", F.date_trunc("week", "event_ts"))

event_counts = (
    weekly.groupBy("repo_name", "week_start")
    .pivot("type", TARGET_EVENT_TYPES)
    .count()
    .fillna(0)
)

# Rename pivoted columns to clean, model-friendly names
rename_map = {
    "PushEvent": "push_count",
    "WatchEvent": "star_count",
    "ForkEvent": "fork_count",
    "IssuesEvent": "issue_count",
    "PullRequestEvent": "pr_count",
    "CreateEvent": "create_count",
    "IssueCommentEvent": "comment_count",
}
for old, new in rename_map.items():
    event_counts = event_counts.withColumnRenamed(old, new)

# Rolling trend features: this week vs trailing 4-week average per repo
# (feeds directly into the abandonment-risk signal XGBoost will learn from)
repo_window = Window.partitionBy("repo_name").orderBy("week_start").rowsBetween(-4, -1)

featured = (
    event_counts.withColumn("push_count_4wk_avg", F.avg("push_count").over(repo_window))
    .withColumn("star_count_4wk_avg", F.avg("star_count").over(repo_window))
    .withColumn(
        "activity_score",
        F.col("push_count") + F.col("pr_count") * 2 + F.col("issue_count"),
    )
    .withColumn(
        "momentum_delta",
        F.col("push_count") - F.coalesce(F.col("push_count_4wk_avg"), F.lit(0)),
    )
)

gold = featured.select(
    "repo_name", "week_start", "push_count", "star_count", "fork_count",
    "issue_count", "pr_count", "create_count", "comment_count",
    "push_count_4wk_avg", "star_count_4wk_avg", "activity_score", "momentum_delta",
)

gold_count = gold.count()
print(f"[GitPulse] Gold layer (training-ready) row count: {gold_count}")

# ---------------------------------------------------------------------------
# 6. Write gold layer to PostgreSQL — this is what XGBoost/Prophet read from
# ---------------------------------------------------------------------------
pg_url = (
    f"jdbc:postgresql://{os.environ.get('PG_HOST', 'postgres')}:"
    f"{os.environ.get('PG_PORT', '5432')}/{os.environ.get('PG_DB', 'gitpulse')}"
)

gold.write.mode("overwrite").format("jdbc").options(
    url=pg_url,
    dbtable="repo_weekly_features",
    user=os.environ.get("PG_USER", "gitpulse"),
    password=os.environ.get("PG_PASSWORD", ""),
    driver="org.postgresql.Driver",
).save()

print("[GitPulse] Gold layer written to Postgres table 'repo_weekly_features'")
print("[GitPulse] Batch processing complete.")

spark.stop()
