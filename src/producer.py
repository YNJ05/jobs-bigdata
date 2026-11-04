"""Producer: reads the job file and sends one offer per Kafka message.

Accepts either a JSON array (like jobs.json) or JSON Lines (one offer per line).
"""
import argparse
import json
import os
import sys
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def read_offers(path):
    with open(path, encoding="utf-8") as fh:
        first = fh.read(1)
        fh.seek(0)
        if first == "[":                       # pretty-printed array
            yield from json.load(fh)
        else:                                  # JSONL: line by line
            for line in fh:
                line = line.strip()
                if line:
                    yield json.loads(line)


def connect(bootstrap, retries=30):
    for attempt in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready ({attempt}/{retries}), retrying...", flush=True)
            time.sleep(2)
    sys.exit("[producer] cannot reach Kafka")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=os.getenv("DATA_FILE", "/app/data/jobs.json"))
    ap.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "job-offers"))
    ap.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--delay", type=float, default=0.1, help="seconds between two messages")
    ap.add_argument("--limit", type=int, default=0, help="stop after N offers (0 = all)")
    args = ap.parse_args()

    producer = connect(args.bootstrap)
    sent = 0
    for offer in read_offers(args.file):
        producer.send(args.topic, key=str(offer["id"]), value=offer)
        sent += 1
        if sent % 50 == 0 or sent == 1:
            print(f"[producer] sent {sent} offers (last: #{offer['id']} {offer['title'][:50]})", flush=True)
        if args.limit and sent >= args.limit:
            break
        time.sleep(args.delay)
    producer.flush()
    print(f"[producer] done: {sent} offers sent to topic '{args.topic}'", flush=True)


if __name__ == "__main__":
    main()
