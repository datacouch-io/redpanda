import os, json, time, re, math, sys
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.environ["RP_BOOTSTRAP"]
USERNAME  = os.environ["RP_USERNAME"]
PASSWORD  = os.environ["RP_PASSWORD"]
TOPIC_INTENTS = os.environ.get("TOPIC_INTENTS", "agent-intents")
TOPIC_OUTPUTS = os.environ.get("TOPIC_OUTPUTS", "agent-outputs")
GROUP_ID      = os.environ.get("GROUP_ID", "agent-worker")
POLL_SECONDS  = int(os.environ.get("POLL_SECONDS", "15"))

consumer = KafkaConsumer(
    TOPIC_INTENTS,
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    group_id=GROUP_ID,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    consumer_timeout_ms=1000,
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-512",
    sasl_plain_username=USERNAME,
    sasl_plain_password=PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
)

def handle(prompt: str):
    m = re.match(r"(?i)calc(?:ulate)?[: ]+(.*)$", prompt)
    expr = m.group(1) if m else None
    if not expr and re.fullmatch(r"[0-9\\s\\+\\-\\*\\/\\(\\)\.]+", prompt):
        expr = prompt
    if expr:
        try:
            val = eval(expr, {"__builtins__": {}}, {"sqrt": math.sqrt, "pow": pow})
            return {"type": "calc", "expression": expr, "value": val}
        except Exception as e:
            return {"type": "calc", "expression": expr, "error": str(e)}
    return {"type": "echo", "echo": prompt}

start = time.time(); processed = 0
while time.time() - start < POLL_SECONDS:
    for msg in consumer:
        try:
            intent = json.loads(msg.value.decode("utf-8"))
            cid = intent.get("correlation_id", "")
            prompt = intent.get("prompt", "")
            outcome = handle(prompt)
            result = {"correlation_id": cid, "outcome": outcome, "ts": time.time()}
            producer.send(TOPIC_OUTPUTS, key=cid, value=result)
            processed += 1
        except Exception as e:
            producer.send(TOPIC_OUTPUTS, key="error", value={"error": str(e)})
    time.sleep(0.2)

producer.flush(); producer.close(); consumer.close()
print(json.dumps({"processed": processed, "window_sec": POLL_SECONDS}))