"""LEVEL 2 - Kafka -> Spark Structured Streaming -> MLlib model -> console."""
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from common import MODEL_PATH, get_spark, max_probability, new_checkpoint, read_kafka_stream, with_text

spark = get_spark("level2-mllib")
model = PipelineModel.load(MODEL_PATH)

stream = with_text(read_kafka_stream(spark))
scored = model.transform(stream)
result = (
    scored.withColumn("confidence", F.round(max_probability("probability"), 3))
    .withColumn("correct", F.col("predicted_domain") == F.col("true_domain"))
    .select("id", F.substring("title", 1, 45).alias("title"), "predicted_domain",
            "confidence", "true_domain", "correct", "split")
)

query = (
    result.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("numRows", 50)
    .option("checkpointLocation", new_checkpoint("level2"))
    .trigger(processingTime="5 seconds")
    .start()
)
query.awaitTermination()
