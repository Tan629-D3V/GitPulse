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
POLL_SECONDS = 5

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
    CURATED_REPOS = [line.strip() for line in f if line.strip()]

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


def poll_repo(repo_name):
    url = f"https://api.github.com/repos/{repo_name}/events"
    resp = requests.get(url, headers=headers, params={"per_page": 100}, timeout=15)

    if resp.status_code == 404:
        return 0
    if resp.status_code in (403, 429):
        wait = int(resp.headers.get("Retry-After", 30))
        print(f"[producer] rate limited, sleeping {wait}s")
        time.sleep(wait)
        return 0
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

        producer.send(TOPIC, e)
        sent += 1

    return sent


if __name__ == "__main__":
    print(
        f"[producer] polling {len(CURATED_REPOS)} curated repos directly, topic '{TOPIC}'"
    )
    while True:
        total_sent = 0
        for repo_name in CURATED_REPOS:
            try:
                total_sent += poll_repo(repo_name)
            except Exception as exc:
                print(f"[producer] error polling {repo_name}: {exc}")
            time.sleep(POLL_SECONDS)

        producer.flush()
        if len(seen_ids) > 20000:
            seen_ids.clear()
        print(
            f"[producer] completed full cycle through {len(CURATED_REPOS)} repos, sent {total_sent} events this cycle"
        )
