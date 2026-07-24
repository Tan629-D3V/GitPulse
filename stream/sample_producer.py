import os
import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "github-events"

EVENT_TYPES = [
    "PushEvent", "WatchEvent", "ForkEvent", "IssuesEvent",
    "PullRequestEvent", "CreateEvent", "IssueCommentEvent",
]

with open("curated_repos.txt") as f:
    CURATED_REPOS = [line.strip() for line in f if line.strip()]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def make_fake_event():
    repo_name = random.choice(CURATED_REPOS)
    event_type = random.choice(EVENT_TYPES)
    return {
        "id": str(uuid.uuid4().int)[:18],  # fake but unique numeric-looking id
        "type": event_type,
        "repo": {"name": repo_name},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


if __name__ == "__main__":
    print(f"[sample-producer] sending synthetic events to '{TOPIC}' via {KAFKA_BOOTSTRAP}")
    count = int(os.environ.get("SAMPLE_EVENT_COUNT", "20"))
    for i in range(count):
        event = make_fake_event()
        producer.send(TOPIC, event)
        print(f"[sample-producer] sent {event['type']} for {event['repo']['name']}")
        time.sleep(1)  # small delay so it looks like a live stream, not a dump
    producer.flush()
    print(f"[sample-producer] done — sent {count} synthetic events")