import os, json, uuid, time
from kafka import KafkaProducer

BOOTSTRAP = os.environ["RP_BOOTSTRAP"]
USERNAME  = os.environ["RP_USERNAME"]
PASSWORD  = os.environ["RP_PASSWORD"]
TOPIC     = os.environ.get("TOPIC_INTENTS", "agent-intents")
PROMPT    = os.environ.get("PROMPT", "calc 2+2*3")

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
)

corr_id = str(uuid.uuid4())
intent = {"correlation_id": corr_id, "prompt": PROMPT, "ts": time.time()}
producer.send(TOPIC, key=corr_id, value=intent)
producer.flush(); producer.close()

print(json.dumps({"status": "queued", "correlation_id": corr_id, "prompt": PROMPT}))