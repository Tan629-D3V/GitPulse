import json
import os
import time

import requests
from kafka import KafkaProducer

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC = "github-events"
POLL_SECONDS = 60

EVENT_TYPES = {
    "PushEvent",
    "WatchEvent",
    "ForkEvent",
    "IssuesEvent",
    "PullRequestEvent",
    "CreateEvent",
    "IssueCommentEvent",
}

with open("curated_repos.txt") as f:
    CURATED_REPOS = {line.strip() for line in f if line.strip()}

if KAFKA_USERNAME:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
else:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

headers = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

seen_ids = set()


def poll_once():
    resp = requests.get(
        "https://api.github.com/events",
        headers=headers,
        params={"per_page": 100},
        timeout=15,
    )
    resp.raise_for_status()
    events = resp.json()

    sent = 0
    for e in events:
        eid = e["id"]
        if eid in seen_ids:
            continue
        seen_ids.add(eid)

        if e["type"] not in EVENT_TYPES:
            continue
        repo_name = e.get("repo", {}).get("name")
        if repo_name not in CURATED_REPOS:
            continue

        producer.send(TOPIC, e)
        sent += 1

    producer.flush()
    if len(seen_ids) > 5000:
        seen_ids.clear()
    print(f"[producer] polled {len(events)} events, sent {sent} matching curated events")


if __name__ == "__main__":
    print(f"[producer] streaming to topic '{TOPIC}' via {KAFKA_BOOTSTRAP}")
    while True:
        try:
            poll_once()
        except Exception as exc:
            print(f"[producer] poll error: {exc}")
        time.sleep(POLL_SECONDS)
