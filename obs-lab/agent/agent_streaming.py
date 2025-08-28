import os, json, time, sqlite3, requests, logging
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer

# -------- env --------
BROKERS       = os.getenv("BROKERS", "redpanda:9092")
TASK_TOPIC    = os.getenv("TASK_TOPIC", "agent.tasks")
ACTION_TOPIC  = os.getenv("ACTION_TOPIC", "agent.actions")
PROM_URL      = os.getenv("PROM_URL", "http://prometheus:9090")
GROUP_ID      = os.getenv("GROUP_ID", "agent-lab")
LAG_THRESHOLD = int(os.getenv("LAG_THRESHOLD", "1000"))
COOLDOWN_SEC  = int(os.getenv("COOLDOWN_SEC", "600"))
CHECK_EVERY_S = int(os.getenv("CHECK_INTERVAL", "20"))

DB_PATH = os.getenv("DB_PATH", "/data/agent.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("agent")

# -------- memory (cooldown + audit) --------
def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS actions(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        kind TEXT NOT NULL,
        group_id TEXT,
        topic TEXT,
        payload TEXT
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_actions_key ON actions(kind, group_id, topic)")
    conn.commit()
    return conn

DB = init_db()

def cooldown_ok(kind, group_id, topic, now):
    cur = DB.cursor()
    cur.execute(
        "SELECT MAX(ts) FROM actions WHERE kind=? AND group_id=? AND topic=?",
        (kind, group_id, topic),
    )
    row = cur.fetchone()
    if row and row[0]:
        last = datetime.fromtimestamp(row[0])
        return (now - last).total_seconds() >= COOLDOWN_SEC
    return True

def record_action(kind, group_id, topic, payload, now):
    cur = DB.cursor()
    cur.execute(
        "INSERT INTO actions(ts, kind, group_id, topic, payload) VALUES (?, ?, ?, ?, ?)",
        (now.timestamp(), kind, group_id, topic, json.dumps(payload)),
    )
    DB.commit()

# -------- tools --------
def prom_instant(query: str):
    r = requests.get(f"{PROM_URL}/api/v1/query", params={"query": query}, timeout=5)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Prom query failed: {data}")
    return data["data"]["result"]

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: (k or "").encode("utf-8"),
        linger_ms=10,
        acks="all",
    )

def kafka_consumer():
    return KafkaConsumer(
        TASK_TOPIC,
        bootstrap_servers=BROKERS,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
    )

# -------- policy loop --------
def evaluate_and_act(producer):
    """
    Policy: If any (group,topic) has rp:consumer_lag_max_5m:group_topic > LAG_THRESHOLD
    and cooldown passed -> emit scale-out action.
    """
    q = 'max by (consumergroup, topic) (rp:consumer_lag_max_5m:group_topic)'
    results = prom_instant(q)
    now = datetime.utcnow()
    triggered = 0
    for series in results:
        labels = series["metric"]
        val = float(series["value"][1])
        group = labels.get("consumergroup", "unknown")
        topic = labels.get("topic", "unknown")
        if val > LAG_THRESHOLD and cooldown_ok("scale-out", group, topic, now):
            action = {
                "ts": now.isoformat() + "Z",
                "action": "scale-out",
                "reason": "consumer_lag",
                "threshold": LAG_THRESHOLD,
                "observed_lag": int(val),
                "group": group,
                "topic": topic,
            }
            producer.send(ACTION_TOPIC, key=f"{group}:{topic}", value=action)
            record_action("scale-out", group, topic, action, now)
            log.info(f"ACTION -> scale-out group={group} topic={topic} lag={int(val)}")
            triggered += 1
    if triggered == 0:
        log.info("No actions; all groups under threshold or in cooldown.")

def handle_task(msg, producer):
    """
    Task schema examples:
      {"type":"check_now"}
      {"type":"set_threshold","value":2000}
    """
    global LAG_THRESHOLD
    t = msg.get("type")
    if t == "check_now":
        evaluate_and_act(producer)
    elif t == "set_threshold":
        v = int(msg.get("value"))
        LAG_THRESHOLD = v
        log.info(f"Threshold updated -> {LAG_THRESHOLD}")
    else:
        log.info(f"Ignored task: {msg}")

def main():
    log.info("Agent starting...")
    prod = kafka_producer()
    cons = kafka_consumer()
    last_check = 0.0
    while True:
        # 1) handle tasks
        for msg in cons:
            try:
                handle_task(msg.value, prod)
            except Exception as e:
                log.error(f"Task error: {e}")

        # 2) periodic check
        now = time.time()
        if now - last_check >= CHECK_EVERY_S:
            try:
                evaluate_and_act(prod)
            except Exception as e:
                log.error(f"Policy error: {e}")
            last_check = now

        time.sleep(1)

if __name__ == "__main__":
    main()