"""LEVEL 1 - Kafka -> Spark Structured Streaming -> keyword classifier -> console."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import get_spark, new_checkpoint, read_kafka_stream
from keywords import classify

spark = get_spark("level1-keywords")

classify_udf = F.udf(classify, StringType())

stream = read_kafka_stream(spark)
result = (
    stream.withColumn("predicted_domain", classify_udf("title", "skills", "description"))
    .withColumn("correct", F.col("predicted_domain") == F.col("true_domain"))
    .select("id", F.substring("title", 1, 45).alias("title"), "predicted_domain", "true_domain", "correct")
)

query = (
    result.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 50)
    .option("checkpointLocation", new_checkpoint("level1"))
    .trigger(processingTime="5 seconds")
    .start()
)
query.awaitTermination()
