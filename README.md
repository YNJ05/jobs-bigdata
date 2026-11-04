# Job offers classification — Kafka + Spark Structured Streaming + MLlib + Elasticsearch/Kibana

Use case 2 of the mini-project: classify each job offer in **Data / Développement / Sécurité / Cloud**.
Dataset: `data/jobs.json` (1,000 offers, 250 per domain). The `domain` field is the ground truth: it is sent
through Kafka as `true_domain` but is **never used as a feature**, only to measure accuracy.

```
jobs.json ──► Producer ──► Kafka topic ──► Spark Structured Streaming ──► console          (levels 1, 2)
 (1 msg/offer)  (python)    "job-offers"     keywords / MLlib model   └──► Elasticsearch ──► Kibana   (level 3)
```

| Level | What it does | Run |
|---|---|---|
| 1 | Keyword scoring (title ×3, skills, description) in Spark | `docker compose --profile level1 up -d --build` |
| 2 | Spark MLlib pipeline: tokenizer → stop-words → TF-IDF → logistic regression | `docker compose --profile level2 up -d --build` |
| 3 | Level 2 + keyword prediction indexed in Elasticsearch, dashboards in Kibana | `docker compose --profile level3 up -d --build` |

Requirements: Docker + Docker Compose v2, **~6 GB RAM** allocated to Docker (level 3 runs Kafka, Spark, Elasticsearch and Kibana).
The first run downloads Spark's Kafka connector (~1–2 min); it is cached in the `ivy` volume afterwards.

## Level 1 — keywords
```bash
docker compose --profile level1 up -d --build
docker compose logs -f level1          # one table per micro-batch (50 offers / 5 s)
```
Columns: `predicted_domain`, `true_domain`, `correct`. Keywords live in `src/keywords.py`.

## Level 2 — Spark MLlib
```bash
docker compose --profile level2 up -d --build
docker compose logs trainer            # metrics + confusion matrix on the held-out test set
docker compose logs -f level2          # predictions with confidence
```
`trainer` runs first (batch) and saves the model in the `models` volume; `level2` then loads it inside the stream.
Split rule: offers with `id % 5 == 0` are the **test set** (20 %); the others train the model. The streaming output
has a `split` column so you can tell whether a prediction was made on an offer seen during training.

## Level 3 — Elasticsearch + Kibana
```bash
docker compose --profile level3 up -d --build
docker compose logs -f level3          # "batch N: 50 offers indexed in Elasticsearch ..."
```
Open **http://localhost:5601** → the data view *Job offers* is created automatically (time field `processed_at`).
Set the time picker to *Last 15 minutes*, then build in **Analytics → Dashboard → Create → Lens**:

1. **Domain share** – Donut: slice by `predicted_domain`, metric *Count*.
2. **Confusion matrix** – Heat map: X `true_domain`, Y `predicted_domain`, cell = *Count*.
3. **Work-mode preference** – Bar stacked: X `predicted_domain`, break down by `workplace_type`
   (same with `employment_type`: internship / contract / full-time).
4. **Top skills per domain** – Horizontal bar: top 10 values of `skills`, break down by `predicted_domain`.
5. **Where are the offers** – Bar: top `location`, break down by `predicted_domain`.
6. **Stream throughput** – Line: *Count* over `processed_at`.
7. **MLlib vs keywords accuracy** – two *Metric* panels with filter `split : "test"` and formulas
   `count(kql='correct : true') / count()` and `count(kql='keyword_correct : true') / count()`.

Quick check without Kibana: `curl "localhost:9200/job-offers/_count"` (should reach 1000).

## Stop / reset
```bash
docker compose --profile level3 down        # use the same profile you started (add -v to wipe volumes)
```
Re-running replays the whole topic; Elasticsearch documents use the offer id as `_id`, so nothing is duplicated.
Change the speed with `--delay` in the `producer` command (seconds between two messages).

## Project layout
```
docker-compose.yml   Kafka (KRaft) · producer · Spark jobs · Elasticsearch · Kibana, grouped by profile
Dockerfile           python 3.11 + Java 17 + PySpark 3.5.1
src/producer.py      reads jobs.json (array or JSONL) and sends one message per offer
src/common.py        schema, Kafka source, split rule
src/keywords.py      level 1 classifier        src/level1_keywords.py
src/train_model.py   level 2 training          src/level2_mllib.py
src/es_utils.py      index mapping + bulk      src/level3_elasticsearch.py   src/setup_kibana.py
```

## Notes and limits
* Expected quality: an equivalent TF-IDF + logistic regression scored ≈ 97.5 % on the test split, and the keywords ≈ 95.5 %.
  The dataset is synthetic and clean, so scores are high; the keyword list was written after reading this dataset,
  so its score is optimistic for unseen offers. Real numbers come from `docker compose logs trainer`.
* Spark runs in `local[2]` mode inside one container (enough for the demo). Level 3 writes through the Elasticsearch
  REST `_bulk` API from `foreachBatch` (micro-batches are collected on the driver: fine for ≤ 50 rows per batch,
  use the `elasticsearch-spark` connector for production volumes).
* Level 4 (live social-network source) and Level 5 (video capsule) are not included.
