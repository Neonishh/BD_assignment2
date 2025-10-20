import json
import csv
import os
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BROKER_IP = "10.147.17.172:9092"
TOPICS = ["topic_net", "topic_disk"]
GROUP_ID = "team-consumer2-fast"

DATA_DIR = "data"
LOG_FILE = "logs/consumer2.log"
NET_FILE = os.path.join(DATA_DIR, "net_data.csv")
DISK_FILE = os.path.join(DATA_DIR, "disk_data.csv")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(filename=LOG_FILE,
                    level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

def write_csv(filename, header, rows):
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

def main():
    print(f"🔗 Connecting to Kafka broker at {BROKER_IP} ...")

    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers=BROKER_IP,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
    except NoBrokersAvailable:
        print("❌ Could not connect to Kafka broker. Check the broker IP or network.")
        return

    for file in [NET_FILE, DISK_FILE]:
        if os.path.exists(file):
            os.remove(file)
            print(f"🧹 Deleted old file: {file}")

    net_rows, disk_rows = [], []
    net_count = disk_count = 0
    total = 0

    print("⚙️ Waiting for messages (Ctrl+C to stop when producer finishes)...")

    try:
        for msg in consumer:
            data = msg.value
            ts = data.get("ts")
            server_id = data.get("server_id")

            if msg.topic == "topic_net":
                net_rows.append([ts, server_id, data.get("net_in"), data.get("net_out")])
                net_count += 1

            elif msg.topic == "topic_disk":
                disk_rows.append([ts, server_id, data.get("disk_io")])
                disk_count += 1

            total = net_count + disk_count

            if total % 5000 == 0:
                print(f"✅ Processed {total} messages (NET={net_count}, DISK={disk_count})")

    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer... (Producer likely finished)")
        logging.info("Consumer2 stopped manually.")

    # --- Sort before writing ---
    net_rows.sort(key=lambda x: x[0])
    disk_rows.sort(key=lambda x: x[0])

    if net_rows:
        write_csv(NET_FILE, ["ts", "server_id", "net_in", "net_out"], net_rows)
    if disk_rows:
        write_csv(DISK_FILE, ["ts", "server_id", "disk_io"], disk_rows)

    consumer.close()

    print(f"📁 Data written successfully to: {NET_FILE} and {DISK_FILE}")
    print(f"✅ Final counts: NET={len(net_rows)}, DISK={len(disk_rows)}, TOTAL={len(net_rows) + len(disk_rows)}")
    logging.info("Consumer2 finished. NET=%d, DISK=%d", len(net_rows), len(disk_rows))

if __name__ == "__main__":
    main()
