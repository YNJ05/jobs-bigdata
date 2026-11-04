"""LEVEL 3 - Kafka -> Spark -> (MLlib + keywords) -> Elasticsearch -> Kibana."""
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import es_utils
from common import MODEL_PATH, get_spark, max_probability, new_checkpoint, read_kafka_stream, with_text
from keywords import classify

spark = get_spark("level3-elasticsearch")
es_utils.wait_for_es()
es_utils.ensure_index()

model = PipelineModel.load(MODEL_PATH)
classify_udf = F.udf(classify, StringType())

stream = with_text(read_kafka_stream(spark))
scored = (
    model.transform(stream)
    .withColumn("confidence", F.round(max_probability("probability"), 4))
    .withColumn("keyword_domain", classify_udf("title", "skills", "description"))
    .withColumn("correct", F.col("predicted_domain") == F.col("true_domain"))
    .withColumn("keyword_correct", F.col("keyword_domain") == F.col("true_domain"))
    .select(*es_utils.DOC_FIELDS)
)


def write_to_es(batch_df, batch_id):
    # Micro-batches are small (<= 50 offers), so collecting them on the driver is fine here.
    rows = [r.asDict() for r in batch_df.collect()]
    n = es_utils.bulk_index(rows)
    ok = sum(1 for r in rows if r["correct"])
    print(f"[level3] batch {batch_id}: {n} offers indexed in Elasticsearch "
          f"({ok}/{n} correctly classified by MLlib)", flush=True)


query = (
    scored.writeStream.foreachBatch(write_to_es)
    .outputMode("append")
    .option("checkpointLocation", new_checkpoint("level3"))
    .trigger(processingTime="5 seconds")
    .start()
)
query.awaitTermination()
