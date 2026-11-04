"""Minimal Elasticsearch helpers using only the standard library (REST API)."""
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
INDEX = os.getenv("ES_INDEX", "job-offers")

MAPPING = {
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
            "company": {"type": "keyword"},
            "location": {"type": "keyword"},
            "workplace_type": {"type": "keyword"},
            "employment_type": {"type": "keyword"},
            "skills": {"type": "keyword"},
            "true_domain": {"type": "keyword"},
            "predicted_domain": {"type": "keyword"},      # MLlib prediction
            "keyword_domain": {"type": "keyword"},        # Level 1 prediction
            "confidence": {"type": "float"},
            "correct": {"type": "boolean"},               # MLlib == truth
            "keyword_correct": {"type": "boolean"},       # keywords == truth
            "split": {"type": "keyword"},                 # train / test
            "processed_at": {"type": "date"},
        }
    }
}

DOC_FIELDS = [
    "id", "title", "company", "location", "workplace_type", "employment_type", "skills",
    "true_domain", "predicted_domain", "keyword_domain", "confidence", "correct",
    "keyword_correct", "split",
]


def _request(method, path, body=None, content_type="application/json", base=None):
    data = body.encode("utf-8") if isinstance(body, str) else body
    req = urllib.request.Request((base or ES_URL) + path, data=data, method=method,
                                 headers={"Content-Type": content_type, "kbn-xsrf": "true"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8") or "{}")


def wait_for_es(timeout=240):
    deadline = time.time() + timeout
    while True:
        try:
            _request("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")
            return
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            if time.time() > deadline:
                raise RuntimeError(f"Elasticsearch not reachable: {exc}")
            time.sleep(3)


def ensure_index():
    """Create the index with an explicit mapping (no-op if it already exists)."""
    try:
        _request("PUT", f"/{INDEX}", json.dumps(MAPPING))
        print(f"[es] index '{INDEX}' created", flush=True)
    except urllib.error.HTTPError as exc:
        if exc.code == 400 and b"resource_already_exists_exception" in exc.read():
            print(f"[es] index '{INDEX}' already exists", flush=True)
        else:
            raise


def build_bulk_body(rows, now=None):
    """Rows (dicts) -> NDJSON body for the _bulk API. _id = offer id, so replays overwrite."""
    now = now or datetime.now(timezone.utc).isoformat()
    lines = []
    for r in rows:
        doc = {k: r.get(k) for k in DOC_FIELDS}
        doc["processed_at"] = now
        lines.append(json.dumps({"index": {"_index": INDEX, "_id": str(r["id"])}}))
        lines.append(json.dumps(doc, ensure_ascii=False))
    return "\n".join(lines) + "\n"


def bulk_index(rows):
    if not rows:
        return 0
    resp = _request("POST", "/_bulk", build_bulk_body(rows), content_type="application/x-ndjson")
    if resp.get("errors"):
        first = next(i for i in resp["items"] if "error" in i["index"])
        raise RuntimeError(f"bulk indexing error: {first['index']['error']}")
    return len(rows)
