import json
import os

import psycopg as psycopg2
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_USERNAME = os.environ.get("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC = "github-events"

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "gitpulse")
PG_USER = os.environ.get("PG_USER", "gitpulse")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "gitpulse")
PG_SSLMODE = os.environ.get("PG_SSLMODE", "prefer")

conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD,
    sslmode=PG_SSLMODE,
)
conn.autocommit = True
cur = conn.cursor()

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS github_events_raw (
        id BIGINT PRIMARY KEY,
        event_type TEXT,
        repo_name TEXT,
        created_at TIMESTAMPTZ,
        payload JSONB
    );
    """
)

consumer_kwargs = dict(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="gitpulse-consumer",
)
if KAFKA_USERNAME:
    consumer_kwargs.update(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
    )

consumer = KafkaConsumer(TOPIC, **consumer_kwargs)

print(f"[consumer] listening on '{TOPIC}' via {KAFKA_BOOTSTRAP}")
for msg in consumer:
    e = msg.value
    try:
        cur.execute(
            """
            INSERT INTO github_events_raw (id, event_type, repo_name, created_at, payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                int(e["id"]),
                e.get("type"),
                e.get("repo", {}).get("name"),
                e.get("created_at"),
                json.dumps(e),
            ),
        )
        print(f"[consumer] inserted {e['id']} ({e.get('type')}) for {e.get('repo', {}).get('name')}")
    except Exception as exc:
        print(f"[consumer] insert error for event {e.get('id')}: {exc}")
