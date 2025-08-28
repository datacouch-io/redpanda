import os, json, time, traceback
from kafka import KafkaConsumer, KafkaProducer
from openai import OpenAI

BROKERS = os.getenv("BROKERS", "redpanda:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "pe.prompts")
OUT_TOPIC = os.getenv("OUT_TOPIC", "pe.outputs")
ERR_TOPIC = os.getenv("ERR_TOPIC", "pe.errors")
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
consumer = KafkaConsumer(IN_TOPIC, bootstrap_servers=[BROKERS], group_id="pe-runner",
                         enable_auto_commit=True, auto_offset_reset="earliest",
                         value_deserializer=lambda b: json.loads(b.decode()))
producer = KafkaProducer(bootstrap_servers=[BROKERS], value_serializer=lambda v: json.dumps(v).encode())

TEMPLATES = {
  "classification": {
    "zero_shot": lambda text: [
      {"role":"system","content":"You are a precise data labeler."},
      {"role":"user","content":f"Label sentiment (Positive|Neutral|Negative) for: {text}\nReturn only one label."}
    ],
    "few_shot": lambda text: [
      {"role":"system","content":"You are a precise data labeler."},
      {"role":"user","content":
        "Examples:\n" \
        "- Review: 'Love battery life' -> Positive\n" \
        "- Review: 'It's fine.' -> Neutral\n" \
        "- Review: 'Mic is terrible' -> Negative\n\n" \
        f"Now label (Positive|Neutral|Negative). Return one label.\nReview: {text}"}
    ]
  },
  "extraction": {
    "zero_shot": lambda text: [
      {"role":"system","content":"Extract fields as VALID JSON only."},
      {"role":"user","content":
        f"Review: {text}\nSchema: {{\\n  'product': string,\\n  'sentiment': 'Positive'|'Neutral'|'Negative',\\n  'reasons': string[]\\n}}\nRules: 1-3 reasons; use 'unknown' if product missing."}
    ],
    "few_shot": lambda text: [
      {"role":"system","content":"Extract fields as VALID JSON only."},
      {"role":"user","content":
        "Example -> Review: 'Battery drains in 3 hours'\\n{" \
        "'product':'unknown','sentiment':'Negative','reasons':['poor battery life']}\\n" \
        "Example -> Review: 'Solid build, average camera'\\n{" \
        "'product':'unknown','sentiment':'Neutral','reasons':['good build quality','average camera']}\\n\n" \
        f"Now process: {text}\\nSchema: {{'product':string,'sentiment':'Positive'|'Neutral'|'Negative','reasons':string[]}}"}
    ]
  },
  "reply": {
    "zero_shot": lambda text: [
      {"role":"system","content":"You write concise, polite support replies (<=90 words), British English."},
      {"role":"user","content":f"Draft a reply that acknowledges the concern and offers ONE next step. No links/markdown. Review: {text}"}
    ],
    "few_shot": lambda text: [
      {"role":"system","content":"You write concise, polite support replies (<=90 words), British English."},
      {"role":"user","content":
        "Style example -> Customer: 'Delivery late; box dented.' Reply: "
        "'Thanks for flagging this—sorry your parcel arrived late and dented. Please share your order ID so we can investigate.'\n\n" \
        f"Now reply to: {text}. One actionable step; no links; no markdown."}
    ]
  }
}

# Use Responses API (new) or Chat Completions; here Responses for simplicity

def call_llm(messages):
    resp = client.responses.create(model=MODEL, input=messages)
    return resp.output_text

for msg in consumer:
    try:
        task = msg.value
        ttype = task.get("task")           # 'classification' | 'extraction' | 'reply'
        mode  = task.get("mode","zero_shot")
        text  = task.get("text","")
        msgs  = TEMPLATES[ttype][mode](text)
        out   = call_llm(msgs)
        producer.send(OUT_TOPIC, {
          "task": ttype, "mode": mode, "input": text, "output": out,
          "ts": int(time.time())
        })
    except Exception as e:
        producer.send(ERR_TOPIC, {
          "error": str(e), "raw": msg.value, "trace": traceback.format_exc(),
          "ts": int(time.time())
        })
