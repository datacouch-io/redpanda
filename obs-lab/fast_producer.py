# sends N messages ASAP
import argparse, time, json, socket
from kafka import KafkaProducer

p = argparse.ArgumentParser()
p.add_argument("--brokers", default="localhost:19092")
p.add_argument("--topic", default="orders")
p.add_argument("--count", type=int, default=200000)
args = p.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.brokers.split(","),
    linger_ms=5, acks="all",
    value_serializer=lambda v: json.dumps(v).encode()
)
host = socket.gethostname()
t0 = time.time()
for i in range(args.count):
    producer.send(args.topic, {"id": i, "host": host, "ts": time.time()})
producer.flush()
elapsed = time.time() - t0
print(f"[producer] sent={args.count} elapsed={elapsed:.2f}s rate~{args.count/elapsed:.0f}/s")