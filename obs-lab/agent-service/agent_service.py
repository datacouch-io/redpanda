import os, json, time, sqlite3, logging, threading
from datetime import datetime, timedelta
from typing import Dict, Any
from flask import Flask, jsonify
import requests
from kafka import KafkaConsumer, KafkaProducer

# ---------- config ----------
BROKERS       = os.getenv("BROKERS", "redpanda:9092")
PROM_URL      = os.getenv("PROM_URL", "http://prometheus:9090")
TASK_TOPIC    = os.getenv("TASK_TOPIC", "agent.tasks")
ACTION_TOPIC  = os.getenv("ACTION_TOPIC", "agent.actions")
JOURNAL_TOPIC = os.getenv("JOURNAL_TOPIC", "agent.journal")
GROUP_ID      = os.getenv("GROUP_ID", "agent-svc")
SERVICE_PORT  = int(os.getenv("SERVICE_PORT", "8088"))
LAG_THRESHOLD = int(os.getenv("LAG_THRESHOLD", "1000"))
COOLDOWN_SEC  = int(os.getenv("COOLDOWN_SEC", "600"))
CHECK_EVERY_S = int(os.getenv("CHECK_INTERVAL", "20"))
USE_RAW_LAG   = os.getenv("USE_RAW_LAG", "0") == "1"  # fallback when recording rule not loaded
DB_PATH       = os.getenv("DB_PATH", "/data/agent.db")

# ---------- logging (structured) ----------
logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger("agent")

def jlog(level: str, msg: str, **kwargs):
    payload = {"ts": datetime.utcnow().isoformat()+"Z", "level": level, "msg": msg}
    payload.update(kwargs)
    getattr(log, level.lower())(json.dumps(payload))

# ---------- memory (cooldown) ----------
def init_db(path: str):
    conn = sqlite3.connect(path, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS actions (
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

DB = init_db(DB_PATH)

# ---------- kafka ----------
producer = KafkaProducer(
    bootstrap_servers=BROKERS.split(","),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: (k or "").encode("utf-8"),
    linger_ms=10, acks="all"
)

consumer = KafkaConsumer(
    TASK_TOPIC,
    bootstrap_servers=BROKERS.split(","),
    group_id=GROUP_ID,
    enable_auto_commit=True,
    auto_offset_reset="latest",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    consumer_timeout_ms=1000,
)

# ---------- prometheus ----------

def prom_instant(query: str):
    r = requests.get(f"{PROM_URL}/api/v1/query", params={"query": query}, timeout=5)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {data}")
    return data["data"]["result"]

# ---------- policy helpers ----------

def cooldown_ok(kind: str, group_id: str, topic: str, now: datetime) -> bool:
    cur = DB.cursor()
    cur.execute("SELECT MAX(ts) FROM actions WHERE kind=? AND group_id=? AND topic=?", (kind, group_id, topic))
    row = cur.fetchone()
    if row and row[0]:
        last = datetime.utcfromtimestamp(row[0])
        return (now - last).total_seconds() >= COOLDOWN_SEC
    return True


def record_action(kind: str, group_id: str, topic: str, payload: Dict[str, Any], now: datetime):
    cur = DB.cursor()
    cur.execute("INSERT INTO actions(ts, kind, group_id, topic, payload) VALUES (?,?,?,?,?)",
                (now.timestamp(), kind, group_id, topic, json.dumps(payload)))
    DB.commit()


def journal(event_type: str, **fields):
    event = {"ts": datetime.utcnow().isoformat()+"Z", "type": event_type}
    event.update(fields)
    producer.send(JOURNAL_TOPIC, key=event_type, value=event)


# ---------- core loop ----------

stop_flag = False
last_check_ts = 0.0
checks = 0


def evaluate_and_act():
    global checks
    now = datetime.utcnow()
    query = 'max by (consumergroup, topic) (rp:consumer_lag_max_5m:group_topic)'
    if USE_RAW_LAG:
        query = 'max by (consumergroup, topic) (kafka_consumergroup_lag)'

    try:
        results = prom_instant(query)
    except Exception as e:
        jlog("warning", "prom_query_error", error=str(e), query=query)
        return

    checks += 1
    fired = 0
    for series in results:
        labels = series.get("metric", {})
        val = float(series.get("value", [None, 0])[1])
        group = labels.get("consumergroup", "unknown")
        topic = labels.get("topic", "unknown")
        if val > LAG_THRESHOLD and cooldown_ok("scale-out", group, topic, now):
            action = {
                "ts": now.isoformat()+"Z",
                "action": "scale-out",
                "reason": "consumer_lag",
                "threshold": LAG_THRESHOLD,
                "observed_lag": int(val),
                "group": group,
                "topic": topic,
            }
            producer.send(ACTION_TOPIC, key=f"{group}:{topic}", value=action)
            record_action("scale-out", group, topic, action, now)
            journal("action", **action)
            jlog("info", "ACTION", kind="scale-out", group=group, topic=topic, lag=int(val))
            fired += 1
    if fired == 0:
        jlog("info", "no_actions", reason="below_threshold_or_cooldown", checks=checks)


# consumer thread: handle tasks

def task_worker():
    jlog("info", "task_worker_started")
    while not stop_flag:
        try:
            for msg in consumer:
                t = msg.value or {}
                journal("task", **t)
                if t.get("type") == "check_now":
                    evaluate_and_act()
                elif t.get("type") == "set_threshold":
                    global LAG_THRESHOLD
                    LAG_THRESHOLD = int(t.get("value", LAG_THRESHOLD))
                    jlog("info", "threshold_updated", value=LAG_THRESHOLD)
                else:
                    jlog("info", "task_ignored", task=t)
        except Exception as e:
            jlog("error", "task_worker_error", error=str(e))
            time.sleep(1)


# scheduler thread: periodic checks

def scheduler_worker():
    jlog("info", "scheduler_started", interval=CHECK_EVERY_S)
    while not stop_flag:
        try:
            evaluate_and_act()
        except Exception as e:
            jlog("error", "scheduler_error", error=str(e))
        time.sleep(CHECK_EVERY_S)


# ---------- health endpoint ----------
app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return jsonify({
        "status": "ok",
        "threshold": LAG_THRESHOLD,
        "cooldown_sec": COOLDOWN_SEC,
        "checks": checks,
        "use_raw_lag": USE_RAW_LAG,
    }), 200


if __name__ == "__main__":
    threading.Thread(target=task_worker, daemon=True).start()
    threading.Thread(target=scheduler_worker, daemon=True).start()
    jlog("info", "agent_service_started", brokers=BROKERS, tasks=TASK_TOPIC, actions=ACTION_TOPIC, journal=JOURNAL_TOPIC)
    app.run(host="0.0.0.0", port=SERVICE_PORT)