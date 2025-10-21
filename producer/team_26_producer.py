import csv
import json
import time
from kafka import KafkaProducer

BROKER = "10.147.17.172:9092"
DATASET = "dataset.csv"
SLEEP_SECONDS = 0   # keep 0 for fastest sending

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",          # Fire-and-forget → FASTEST mode
    linger_ms=5,     # Tiny batching for speed boost
    batch_size=32768 # Send bigger batches instead of 1 by 1
)

counter = 0

with open(DATASET, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        ts = row["ts"]
        server_id = row["server_id"]

        # CPU → topic-cpu
        producer.send("topic_cpu", key=server_id, value={
            "ts": ts, "server_id": server_id, "cpu_pct": float(row["cpu_pct"])
        })

        # Memory → topic-mem
        producer.send("topic_mem", key=server_id, value={
            "ts": ts, "server_id": server_id, "mem_pct": float(row["mem_pct"])
        })

        # Network → topic-net
        producer.send("topic_net", key=server_id, value={
            "ts": ts, "server_id": server_id,
            "net_in": float(row["net_in"]),
            "net_out": float(row["net_out"]),
        })

        # Disk → topic-disk
        producer.send("topic_disk", key=server_id, value={
            "ts": ts, "server_id": server_id, "disk_io": float(row["disk_io"])
        })

        counter += 4  # Four messages per row

        # Print only occasionally
        if counter % 5000 == 0:
            print(f"Sent {counter} messages so far...")

        time.sleep(SLEEP_SECONDS)

producer.flush()
print("Producer finished sending data.")




