"""Shared helpers for the Spark Structured Streaming jobs."""
import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "job-offers")
MODEL_PATH = os.getenv("MODEL_PATH", "/models/jobs_lr")
DATA_FILE = os.getenv("DATA_FILE", "/app/data/jobs.json")

DOMAINS = ["Data", "Développement", "Sécurité", "Cloud"]

# Structure of one JSON offer sent by the producer.
JOB_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("title", StringType()),
    StructField("company", StringType()),
    StructField("location", StringType()),
    StructField("domain", StringType()),          # ground truth, used ONLY for evaluation
    StructField("skills", ArrayType(StringType())),
    StructField("salary_range", StringType()),
    StructField("workplace_type", StringType()),
    StructField("employment_type", StringType()),
    StructField("description", StringType()),
])


def get_spark(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark, max_per_trigger=50):
    """Kafka topic -> streaming DataFrame with one column per JSON field."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_per_trigger)   # small micro-batches -> visible stream
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.from_json(F.col("value").cast("string"), JOB_SCHEMA).alias("j"))
        .select("j.*")
        .withColumnRenamed("domain", "true_domain")        # never used as a feature
        .withColumn("split", split_col())
    )


def split_col():
    """Deterministic split: ids divisible by 5 are the held-out test set (20 %)."""
    return F.when(F.col("id") % 5 == 0, "test").otherwise("train")


def with_text(df):
    """Single text column used by the ML pipeline: title + skills + description."""
    return df.withColumn(
        "text",
        F.concat_ws(" ", F.col("title"), F.concat_ws(" ", F.col("skills")), F.col("description")),
    )


def max_probability(col):
    """Highest class probability of an MLlib `probability` vector (plain UDF: no pandas/pyarrow needed)."""
    udf = F.udf(lambda v: float(max(v.toArray())) if v is not None else None, DoubleType())
    return udf(col)


def new_checkpoint(name):
    # A fresh directory each run: restarting a container replays the topic from the start.
    return tempfile.mkdtemp(prefix=f"chk-{name}-")
