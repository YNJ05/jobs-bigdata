"""Creates the Elasticsearch index and the Kibana data view (run once, idempotent)."""
import json
import os
import time
import urllib.error

import es_utils

KIBANA_URL = os.getenv("KIBANA_URL", "http://kibana:5601")

es_utils.wait_for_es()
es_utils.ensure_index()

deadline = time.time() + 300
while True:
    try:
        status = es_utils._request("GET", "/api/status", base=KIBANA_URL)
        if status.get("status", {}).get("overall", {}).get("level") == "available":
            break
    except (urllib.error.URLError, ConnectionError, OSError):
        pass
    if time.time() > deadline:
        raise SystemExit("[setup] Kibana not ready after 300 s")
    time.sleep(5)

body = {
    "data_view": {"id": "job-offers", "title": es_utils.INDEX, "name": "Job offers",
                  "timeFieldName": "processed_at"},
    "override": True,
}
es_utils._request("POST", "/api/data_views/data_view", json.dumps(body), base=KIBANA_URL)
print(f"[setup] Kibana data view 'Job offers' ready -> {KIBANA_URL}/app/discover", flush=True)
