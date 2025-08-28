import os, json, threading, queue, time, uuid
from datetime import datetime
import pandas as pd
import streamlit as st
from kafka import KafkaProducer, KafkaConsumer

BROKERS = os.getenv("BROKERS", "redpanda:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "pe.prompts")
OUT_TOPIC = os.getenv("OUT_TOPIC", "pe.outputs")
ERR_TOPIC = os.getenv("ERR_TOPIC", "pe.errors")

st.set_page_config(page_title="Prompt Studio", layout="wide")
st.title("🧪 Prompt Studio (Redpanda + OpenAI runner)")

# --- Kafka clients ---
@st.cache_resource
def get_producer():
    return KafkaProducer(bootstrap_servers=BROKERS.split(","), value_serializer=lambda v: json.dumps(v).encode())

producer = get_producer()

# Threaded consumers push into queues
out_q, err_q = queue.Queue(maxsize=5000), queue.Queue(maxsize=2000)
stop_event = threading.Event()

@st.cache_resource
def start_consumers():
    def tail(topic, q):
        c = KafkaConsumer(topic,
                          bootstrap_servers=BROKERS.split(","),
                          group_id="pe-studio",
                          enable_auto_commit=True,
                          auto_offset_reset="latest",
                          value_deserializer=lambda b: json.loads(b.decode()),
                          consumer_timeout_ms=1000)
        while not stop_event.is_set():
            try:
                for m in c:
                    try:
                        q.put_nowait(m.value)
                    except queue.Full:
                        pass
            except Exception:
                time.sleep(0.5)
        c.close()
    t1 = threading.Thread(target=tail, args=(OUT_TOPIC, out_q), daemon=True)
    t2 = threading.Thread(target=tail, args=(ERR_TOPIC, err_q), daemon=True)
    t1.start(); t2.start()
    return True

start_consumers()

# Session state for tables
if "out_rows" not in st.session_state:
    st.session_state.out_rows = []
if "err_rows" not in st.session_state:
    st.session_state.err_rows = []

# --- Controls ---
with st.sidebar:
    st.header("Send a prompt")
    task = st.selectbox("Task", ["classification", "extraction", "reply"], index=0)
    mode = st.radio("Mode", ["zero_shot", "few_shot"], index=0, horizontal=True)
    text = st.text_area("Input text", height=120, placeholder="Type a review or request…")
    colA, colB = st.columns([1,1])
    with colA:
        if st.button("Send", type="primary", use_container_width=True, disabled=not text.strip()):
            msg = {"id": str(uuid.uuid4()), "task": task, "mode": mode, "text": text.strip(), "ts": int(time.time())}
            producer.send(IN_TOPIC, msg)
            st.success("Queued to pe.prompts")
    with colB:
        uploaded = st.file_uploader("Batch CSV (column: review)", type=["csv"], accept_multiple_files=False)
        if uploaded is not None:
            df = pd.read_csv(uploaded)
            if "review" in df.columns:
                sent = 0
                for r in df["review"].dropna().tolist():
                    msg = {"id": str(uuid.uuid4()), "task": task, "mode": mode, "text": str(r), "ts": int(time.time())}
                    producer.send(IN_TOPIC, msg); sent += 1
                st.success(f"Queued {sent} rows to pe.prompts")
            else:
                st.error("CSV must contain a 'review' column")

# --- Live tables ---
col1, col2 = st.columns([3,1])
with col1:
    st.subheader("pe.outputs (model results)")
    # Drain queue
    drained = 0
    while not out_q.empty() and drained < 1000:
        st.session_state.out_rows.append(out_q.get())
        # keep last 1000
        st.session_state.out_rows = st.session_state.out_rows[-1000:]
        drained += 1
    out_df = pd.DataFrame(st.session_state.out_rows)
    if not out_df.empty:
        out_df = out_df.sort_values("ts", ascending=False)
        st.dataframe(out_df, use_container_width=True, height=360)
        st.download_button("Download outputs CSV", out_df.to_csv(index=False), file_name="pe_outputs.csv")
    else:
        st.info("No outputs yet. Send a prompt or run a batch.")

    st.subheader("pe.errors")
    drained = 0
    while not err_q.empty() and drained < 1000:
        st.session_state.err_rows.append(err_q.get())
        st.session_state.err_rows = st.session_state.err_rows[-1000:]
        drained += 1
    err_df = pd.DataFrame(st.session_state.err_rows)
    if not err_df.empty:
        err_df = err_df.sort_values("ts", ascending=False)
        st.dataframe(err_df, use_container_width=True, height=200)
    else:
        st.success("No errors.")

with col2:
    st.subheader("Status")
    st.metric("Outputs", len(st.session_state.out_rows))
    st.metric("Errors", len(st.session_state.err_rows))
    st.caption(f"Brokers: {BROKERS}\n\nIn: {IN_TOPIC}\nOut: {OUT_TOPIC}\nErr: {ERR_TOPIC}")
    st.caption("Tip: Console → Topics → pe.outputs to inspect records.")
