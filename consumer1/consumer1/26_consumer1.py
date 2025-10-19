import json
import csv
import os
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Kafka Configuration ---
BROKER_IP = "10.147.17.172:9092"
TOPICS = ["topic_cpu", "topic_mem"]
GROUP_ID = "team-consumer1"

# --- Directory Setup ---
DATA_DIR = "data"
LOG_FILE = "logs/consumer1.log"
CPU_FILE = os.path.join(DATA_DIR, "cpu_data.csv")
MEM_FILE = os.path.join(DATA_DIR, "mem_data.csv")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# --- Logging Setup ---
logging.basicConfig(filename=LOG_FILE,
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


def write_csv(filename, header, rows):
    """Write rows to CSV (overwrite file each run)."""
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main():
    print(f"Connecting to Kafka broker at {BROKER_IP} ...")

    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=BROKER_IP,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",   # read from beginning if no committed offset
            enable_auto_commit=True,         # ensures messages processed once
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
    except NoBrokersAvailable:
        print("Could not connect to Kafka broker. Check the broker IP or network.")
        return

    print(f"Connected! Listening to topics: {TOPICS}")
    logging.info("Consumer1 started. Listening to %s", TOPICS)

    # --- Clean old CSVs before run ---
    for file in [CPU_FILE, MEM_FILE]:
        if os.path.exists(file):
            os.remove(file)

    cpu_rows, mem_rows = [], []
    cpu_count = mem_count = 0
    total = 0

    print("Waiting for messages (click Ctrl+C to stop when producer finishes)...")

    try:
        for msg in consumer:
            data = msg.value
            ts = data.get("ts")
            server_id = data.get("server_id")

            if msg.topic == "topic_cpu":
                cpu_rows.append([ts, server_id, data.get("cpu_pct")])
                cpu_count += 1

            elif msg.topic == "topic_mem":
                mem_rows.append([ts, server_id, data.get("mem_pct")])
                mem_count += 1

            total = cpu_count + mem_count

            # Print progress every 5000 messages
            if total % 5000 == 0:
                print(f"Processed {total} messages (CPU={cpu_count}, MEM={mem_count})")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
        logging.info("Consumer1 stopped manually.")

    # --- Align and sort before writing ---
    min_len = min(len(cpu_rows), len(mem_rows))
    cpu_rows = cpu_rows[:min_len]
    mem_rows = mem_rows[:min_len]

    cpu_rows.sort(key=lambda x: x[0])
    mem_rows.sort(key=lambda x: x[0])

    # --- Write final CSVs ---
    if cpu_rows:
        write_csv(CPU_FILE, ["ts", "server_id", "cpu_pct"], cpu_rows)
    if mem_rows:
        write_csv(MEM_FILE, ["ts", "server_id", "mem_pct"], mem_rows)

    consumer.close()

    print(f"Data written successfully to: {CPU_FILE} and {MEM_FILE}")
    print(f"Final counts: CPU={len(cpu_rows)}, MEM={len(mem_rows)}, TOTAL={len(cpu_rows) + len(mem_rows)}")
    logging.info("Consumer1 finished. CPU=%d, MEM=%d", len(cpu_rows), len(mem_rows))


if __name__ == "__main__":
    main()

