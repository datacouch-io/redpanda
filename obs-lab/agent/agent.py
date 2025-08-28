#!/usr/bin/env python3
import ast, operator, sqlite3, json, time, re
from collections import deque
from dataclasses import dataclass
from typing import Callable, Dict, Any, Optional, Tuple, List
from jsonschema import validate, ValidationError
from rich.console import Console
from rich.table import Table

console = Console()

# ---------- Tool Contract ----------
@dataclass
class Tool:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Callable[[Dict[str, Any]], Dict[str, Any]]

    def __call__(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        validate(instance=payload, schema=self.input_schema)
        result = self.handler(payload)
        return {"tool": self.name, "ok": True, "result": result}

class ToolRegistry:
    def __init__(self):
        self._tools: Dict[str, Tool] = {}

    def register(self, tool: Tool):
        self._tools[tool.name] = tool

    def call(self, name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if name not in self._tools:
            raise KeyError(f"Unknown tool: {name}")
        return self._tools[name](payload)

    def catalog(self) -> List[Dict[str, str]]:
        return [{"name": t.name, "description": t.description} for t in self._tools.values()]

# ---------- Memory ----------
class MemoryStore:
    """Short-term memory (bounded) + Long-term memory (SQLite)."""
    def __init__(self, db_path="agent_memory.db", st_capacity=10):
        self.st = deque(maxlen=st_capacity)
        self.db = sqlite3.connect(db_path)
        self._init_db()

    def _init_db(self):
        cur = self.db.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            kind TEXT NOT NULL,
            text TEXT,
            payload TEXT
        )""")
        self.db.commit()

    def remember(self, kind: str, text: str = "", payload: Optional[Dict[str, Any]] = None):
        entry = {"ts": time.time(), "kind": kind, "text": text, "payload": payload or {}}
        self.st.append(entry)
        cur = self.db.cursor()
        cur.execute("INSERT INTO events (ts, kind, text, payload) VALUES (?, ?, ?, ?)",
                    (entry["ts"], kind, text, json.dumps(entry["payload"])))
        self.db.commit()
        return entry

    def recall(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        cur = self.db.cursor()
        q = f"%{query.lower()}%"
        cur.execute("""
          SELECT ts, kind, text, payload FROM events
          WHERE lower(text) LIKE ? OR lower(payload) LIKE ?
          ORDER BY ts DESC LIMIT ?""", (q, q, limit))
        rows = cur.fetchall()
        return [{"ts": ts, "kind": kind, "text": text, "payload": json.loads(payload)} for ts, kind, text, payload in rows]

# ---------- Safe Calculator ----------
class SafeCalc:
    ops = {
        ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
        ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv,
        ast.Pow: operator.pow, ast.Mod: operator.mod,
        ast.USub: operator.neg, ast.UAdd: operator.pos
    }
    def eval(self, expr: str) -> float:
        node = ast.parse(expr, mode="eval").body
        return self._eval(node)

    def _eval(self, node):
        if isinstance(node, ast.Num):
            return node.n
        if isinstance(node, ast.UnaryOp) and type(node.op) in self.ops:
            return self.ops[type(node.op)](self._eval(node.operand))
        if isinstance(node, ast.BinOp) and type(node.op) in self.ops:
            return self.ops[type(node.op)](self._eval(node.left), self._eval(node.right))
        raise ValueError("Disallowed expression")

calc = SafeCalc()

# ---------- Tools (handlers) ----------
def t_calculator(payload):
    value = calc.eval(payload["expression"])
    return {"expression": payload["expression"], "value": value}

def t_note(payload):
    text = payload["text"].strip()
    return {"saved": True, "text": text}

def t_todo_add(payload):
    item = payload["item"].strip()
    return {"added": item}

def t_memory_search(payload):
    results = AGENT.memory.recall(payload["query"], limit=payload.get("limit", 5))
    return {"matches": results}

# ---------- Agent ----------
@dataclass
class Action:
    tool: str
    payload: Dict[str, Any]
    rationale: str

class Agent:
    def __init__(self, memory: MemoryStore, tools: ToolRegistry):
        self.memory = memory
        self.tools = tools

    # --- Reason: simple rule-based planner (offline/deterministic) ---
    def reason(self, observation: str) -> Action:
        text = observation.strip()

        # calculator
        m = re.match(r"(?i)calc(?:ulate)?[: ]+(.*)$", text)
        if m:
            expr = m.group(1)
            return Action(
                tool="calculator",
                payload={"expression": expr},
                rationale=f"User asked to calculate '{expr}'."
            )

        # add todo
        m = re.match(r"(?i)todo[: ]+(.*)$", text)
        if m:
            item = m.group(1)
            return Action(
                tool="todo.add",
                payload={"item": item},
                rationale=f"User wants to add a todo: {item}"
            )

        # note
        m = re.match(r"(?i)remember|note[: ]+(.*)$", text)
        if m:
            content = text.split(" ", 1)[1] if " " in text else text
            return Action(
                tool="note",
                payload={"text": content},
                rationale=f"Store note: {content}"
            )

        # memory search
        m = re.match(r"(?i)search[: ]+(.*)$", text)
        if m:
            q = m.group(1)
            return Action(
                tool="memory.search",
                payload={"query": q, "limit": 5},
                rationale=f"Search memory for '{q}'."
            )

        # fallback: try to infer math expression
        if re.fullmatch(r"[0-9\.\s\+\-\*\/\(\)%]+", text):
            return Action(
                tool="calculator",
                payload={"expression": text},
                rationale="Input looks like a math expression."
            )

        # final fallback: note it
        return Action(
            tool="note",
            payload={"text": text},
            rationale="No direct tool detected; keeping as a note."
        )

    # --- Act: call tool ---
    def act(self, action: Action) -> Dict[str, Any]:
        return self.tools.call(action.tool, action.payload)

    # --- Learn: write to memory (ST + LT) ---
    def learn(self, observation: str, action: Action, outcome: Dict[str, Any]):
        self.memory.remember(
            kind="observation", text=observation, payload={}
        )
        self.memory.remember(
            kind="action", text=action.rationale,
            payload={"tool": action.tool, "payload": action.payload}
        )
        self.memory.remember(
            kind="outcome", text=json.dumps(outcome)[:200], payload=outcome
        )

    # --- Loop: REPL ---
    def loop(self):
        console.print("[bold cyan]Minimal Agent[/] — type 'help' or 'exit'\n")
        console.print("[dim]Commands: calc 2+2, todo call mom, note buy milk, search milk[/dim]")
        while True:
            try:
                obs = console.input("[bold magenta]You> [/]")
            except (KeyboardInterrupt, EOFError):
                print()
                break
            if obs.strip().lower() in {"exit", "quit"}:
                break
            if obs.strip().lower() == "help":
                table = Table(title="Available tools")
                table.add_column("Tool")
                table.add_column("Description")
                for t in self.tools.catalog():
                    table.add_row(t["name"], t["description"])
                console.print(table)
                continue

            try:
                action = self.reason(obs)
                outcome = self.act(action)
                self.learn(obs, action, outcome)
                console.print(f"[green]✓[/] [b]{action.tool}[/] → {json.dumps(outcome['result'], ensure_ascii=False)}")
            except ValidationError as ve:
                console.print(f"[red]Schema error:[/] {ve.message}")
            except Exception as e:
                console.print(f"[red]Error:[/] {e}")

# ---------- Wire it up ----------
def build_agent(db_path="agent_memory.db") -> Agent:
    memory = MemoryStore(db_path=db_path, st_capacity=10)
    tools = ToolRegistry()
    tools.register(Tool(
        name="calculator",
        description="Safe arithmetic: calc 2+2*3, or 'calculate: 10/4'",
        input_schema={"type": "object", "properties": {"expression": {"type": "string"}}, "required": ["expression"]},
        handler=t_calculator
    ))
    tools.register(Tool(
        name="note",
        description="Save a note or memory: 'note buy milk'",
        input_schema={"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
        handler=t_note
    ))
    tools.register(Tool(
        name="todo.add",
        description="Add a todo item: 'todo call mom'",
        input_schema={"type": "object", "properties": {"item": {"type": "string"}}, "required": ["item"]},
        handler=t_todo_add
    ))
    tools.register(Tool(
        name="memory.search",
        description="Search long-term memory: 'search milk'",
        input_schema={"type": "object", "properties": {"query": {"type": "string"}, "limit": {"type": "integer", "minimum": 1}}, "required": ["query"]},
        handler=t_memory_search
    ))
    return Agent(memory, tools)

# Make the agent globally visible for tool handlers needing memory
AGENT = build_agent()

if __name__ == "__main__":
    AGENT.loop()