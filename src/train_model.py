"""LEVEL 2 (batch part) - train a Spark MLlib text classifier on jobs.json.

Pipeline: RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF
          -> LogisticRegression (multinomial) -> IndexToString
Train on 80 % of the offers (id % 5 != 0), evaluate on the other 20 %.
"""
import json
import os

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, IndexToString, RegexTokenizer, StopWordsRemover
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import DATA_FILE, DOMAINS, MODEL_PATH, get_spark, split_col, with_text
from keywords import classify

spark = get_spark("train-mllib")

df = spark.read.option("multiLine", True).json(DATA_FILE)
df = with_text(df).withColumn("split", split_col())

label = None
for i, dom in enumerate(DOMAINS):
    cond = F.col("domain") == dom
    label = F.when(cond, float(i)) if label is None else label.when(cond, float(i))
df = df.withColumn("label", label)

train, test = df.filter("split = 'train'"), df.filter("split = 'test'")
print(f"[train] train={train.count()} test={test.count()}")

pipeline = Pipeline(stages=[
    RegexTokenizer(inputCol="text", outputCol="tokens", pattern="[^a-z0-9+#]+", minTokenLength=2),
    StopWordsRemover(inputCol="tokens", outputCol="filtered"),
    HashingTF(inputCol="filtered", outputCol="tf", numFeatures=1 << 14),
    IDF(inputCol="tf", outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label", family="multinomial",
                       maxIter=50, regParam=0.01),
    IndexToString(inputCol="prediction", outputCol="predicted_domain", labels=DOMAINS),
])
model = pipeline.fit(train)

pred = model.transform(test)
metrics = {}
for name in ("accuracy", "weightedPrecision", "weightedRecall", "f1"):
    metrics[name] = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName=name).evaluate(pred)

kw_udf = F.udf(classify, StringType())
metrics["keyword_baseline_accuracy"] = (
    test.withColumn("kw", kw_udf("title", "skills", "description"))
    .filter(F.col("kw") == F.col("domain")).count() / test.count()
)

print("[train] metrics on the held-out test set:")
for k, v in metrics.items():
    print(f"    {k:28s} {v:.4f}")
print("[train] confusion matrix (rows = true domain, columns = predicted):")
pred.groupBy("domain").pivot("predicted_domain", DOMAINS).count().na.fill(0).show(truncate=False)

model.write().overwrite().save(MODEL_PATH)
with open(os.path.join(os.path.dirname(MODEL_PATH), "metrics.json"), "w") as fh:
    json.dump(metrics, fh, indent=2)
print(f"[train] model saved to {MODEL_PATH}")
spark.stop()
