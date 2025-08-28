# same group for lag; slow on purpose
import argparse, time, json
from kafka import KafkaConsumer

p = argparse.ArgumentParser()
p.add_argument("--brokers", default="localhost:19092")
p.add_argument("--topic", default="orders")
p.add_argument("--group", default="cg-agent-demo")
p.add_argument("--sleep-ms", type=int, default=250)  # slow down
args = p.parse_args()

consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers=args.brokers.split(","),
    group_id=args.group,
    enable_auto_commit=True,
    auto_offset_reset="earliest",
    value_deserializer=lambda b: json.loads(b.decode())
)

count = 0
for msg in consumer:
    count += 1
    if count % 100 == 0:
        print(f"[consumer] processed={count}")
    time.sleep(args.sleep_ms / 1000.0)