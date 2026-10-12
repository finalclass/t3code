#!/usr/bin/env node
// t3-alpha: minimal console client for a T3 Code server (tested against wire format of v0.0.45).
// No dependencies; needs Node >= 22 (global fetch + WebSocket).
// Wire: Effect RPC over WebSocket, JSON serialization (apps/server/src/ws.ts RpcSerialization.layerJson).
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";

const CONFIG_PATH = process.env.T3_ALPHA_CONFIG || join(homedir(), ".config", "t3-alpha.json");

export function loadConfig() {
  const c = existsSync(CONFIG_PATH) ? JSON.parse(readFileSync(CONFIG_PATH, "utf8")) : {};
  if (process.env.T3_ALPHA_URL) c.baseUrl = process.env.T3_ALPHA_URL;
  if (process.env.T3_ALPHA_TOKEN) c.token = process.env.T3_ALPHA_TOKEN;
  c.baseUrl ||= "http://127.0.0.1:5773";
  c.model ||= { instanceId: "codex", model: "gpt-5-codex", options: [] };
  c.runtimeMode ||= "full-access";
  c.interactionMode ||= "default";
  return c;
}
function saveConfig(c) {
  mkdirSync(dirname(CONFIG_PATH), { recursive: true });
  writeFileSync(CONFIG_PATH, JSON.stringify(c, null, 2) + "\n", { mode: 0o600 });
}

// ---- HTTP auth ----
async function exchangePairingCode(baseUrl, code) {
  // POST /oauth/token (packages/contracts/src/environmentHttp.ts, auth.ts AuthTokenExchangeRequest)
  const body = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
    subject_token: code,
    subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
    requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
    client_label: "t3-alpha CLI",
    client_device_type: "desktop",
  });
  const r = await fetch(new URL("/oauth/token", baseUrl), {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body,
  });
  const t = await r.text();
  if (!r.ok) throw new Error(`token exchange HTTP ${r.status}: ${t}`);
  return JSON.parse(t);
}
async function wsTicket(cfg) {
  const r = await fetch(new URL("/api/auth/websocket-ticket", cfg.baseUrl), {
    method: "POST",
    headers: { authorization: `Bearer ${cfg.token}` },
  });
  const t = await r.text();
  if (!r.ok) throw new Error(`websocket-ticket HTTP ${r.status}: ${t}`);
  return JSON.parse(t).ticket;
}

// ---- Effect RPC client (JSON) ----
export class RpcClient {
  constructor(ws) {
    this.ws = ws; this.next = 0; this.pending = new Map();
    ws.addEventListener("message", (ev) => this.onMessage(String(ev.data)));
    ws.addEventListener("close", () => {
      for (const p of this.pending.values()) p.reject(new Error("socket closed"));
      this.pending.clear();
    });
  }
  static async connect(cfg) {
    const ticket = await wsTicket(cfg);
    const u = new URL("/ws", cfg.baseUrl);
    u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
    u.searchParams.set("wsTicket", ticket);
    u.searchParams.set("clientSurface", "web");
    u.searchParams.set("clientDeviceType", "desktop");
    u.searchParams.set("orchestrationProtocol", "1");
    const ws = new WebSocket(u);
    await new Promise((res, rej) => {
      ws.addEventListener("open", res, { once: true });
      ws.addEventListener("error", () => rej(new Error("WebSocket connect failed")), { once: true });
    });
    const c = new RpcClient(ws);
    c.ping = setInterval(() => ws.readyState === 1 && ws.send(JSON.stringify({ _tag: "Ping" })), 10000);
    return c;
  }
  close() { clearInterval(this.ping); this.ws.close(); }
  send(o) { this.ws.send(JSON.stringify(o)); }
  onMessage(raw) {
    let msgs; try { msgs = JSON.parse(raw); } catch { return; }
    for (const m of Array.isArray(msgs) ? msgs : [msgs]) {
      const p = this.pending.get(String(m.requestId));
      if (m._tag === "Chunk" && p) {
        this.send({ _tag: "Ack", requestId: String(m.requestId) });
        for (const v of m.values) p.onChunk?.(v);
      } else if (m._tag === "Exit" && p) {
        this.pending.delete(String(m.requestId));
        if (m.exit._tag === "Success") p.resolve(m.exit.value);
        else p.reject(new Error("RPC failure: " + JSON.stringify(m.exit.cause)));
      } else if (m._tag === "Defect") {
        for (const q of this.pending.values()) q.reject(new Error("RPC defect: " + JSON.stringify(m.defect)));
        this.pending.clear();
      }
    }
  }
  call(tag, payload, onChunk) {
    const id = String(this.next++);
    const promise = new Promise((resolve, reject) => this.pending.set(id, { resolve, reject, onChunk }));
    this.send({ _tag: "Request", id, tag, payload, headers: [] });
    return { id, promise, interrupt: () => this.send({ _tag: "Interrupt", requestId: id, interruptors: [] }) };
  }
  request(tag, payload) { return this.call(tag, payload).promise; }
}

// ---- command builders (contracts/src/orchestration.ts @ v0.0.45) ----
const now = () => new Date().toISOString();
export function buildThreadCreate(cfg, { projectId, title, threadId = randomUUID() }) {
  return {
    type: "thread.create", commandId: randomUUID(), threadId, projectId, title,
    modelSelection: cfg.model, runtimeMode: cfg.runtimeMode, interactionMode: cfg.interactionMode,
    branch: null, worktreePath: null, createdAt: now(),
  };
}
export function buildTurnStart(cfg, { threadId, text }) {
  return {
    type: "thread.turn.start", commandId: randomUUID(), threadId,
    message: { messageId: randomUUID(), role: "user", text, attachments: [] },
    runtimeMode: cfg.runtimeMode, interactionMode: cfg.interactionMode, createdAt: now(),
  };
}

// ---- reading ----
async function threadSnapshot(rpc, threadId) {
  return new Promise((resolve, reject) => {
    const h = rpc.call("orchestration.subscribeThread", { threadId }, (item) => {
      if (item.kind === "snapshot") { h.interrupt(); resolve(item.snapshot); }
    });
    h.promise.catch(reject);
    setTimeout(() => reject(new Error("timeout waiting for snapshot")), 15000);
  });
}
function messagesOf(snap) { return snap.thread?.messages ?? snap.messages ?? []; }
function printStatus(snap) {
  const t = snap.thread;
  const lt = t.latestTurn;
  console.log(`thread:  ${t.id}\ntitle:   ${t.title}\nproject: ${t.projectId}`);
  console.log(`turn:    ${lt ? `${lt.state} (requested ${lt.requestedAt}${lt.completedAt ? `, done ${lt.completedAt}` : ""})` : "none"}`);
  console.log(`messages: ${messagesOf(snap).length}`);
}
function printMessages(snap, all) {
  const ms = messagesOf(snap).filter((m) => m.role === "user" || m.role === "assistant");
  const list = all ? ms : ms.filter((m) => m.role === "assistant").slice(-1);
  if (!list.length) console.log("(no assistant reply yet)");
  for (const m of list) console.log(`--- ${m.role} ${m.createdAt}${m.streaming ? " (streaming)" : ""}\n${m.text}`);
}
async function waitForTurn(rpc, threadId, timeoutMs) {
  return new Promise((resolve) => {
    let done = false;
    const h = rpc.call("orchestration.subscribeThread", { threadId }, (item) => {
      if (item.kind === "event") {
        const e = item.event;
        if (e.type === "thread.message.assistant.delta") process.stdout.write(e.payload?.delta ?? e.payload?.text ?? "");
        if (/turn.*(completed|failed|interrupted)|session-set/.test(e.type) && !done) {
          if (/completed|failed|interrupted/.test(e.type)) { done = true; h.interrupt(); process.stdout.write("\n"); resolve(e.type); }
        }
      }
    });
    setTimeout(() => { if (!done) { h.interrupt(); resolve("timeout"); } }, timeoutMs);
  });
}

// ---- CLI ----
function parseArgs(argv) {
  const o = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) { const k = a.slice(2); const v = argv[i + 1]?.startsWith("--") || argv[i + 1] === undefined ? true : argv[++i]; o[k] = v; }
    else o._.push(a);
  }
  return o;
}
const USAGE = `t3-alpha <command> [options]
  login --url <http://127.0.0.1:5773> (--token <bearer> | --pair <pairing-code>)
  projects                                   list projects (id, title, root)
  create --project <id> --title <t> --prompt <p> [--wait <sec>]
  send   --thread <id> --prompt <p> [--wait <sec>]
  status --thread <id>
  read   --thread <id> [--all]
  batch  --file <path>     one JSON object per line: {"cmd":"create",...}
  frames --project <id> --title <t> --prompt <p>   print frames only (offline)
Config: ${CONFIG_PATH} (env T3_ALPHA_URL / T3_ALPHA_TOKEN override)`;

async function run(args, cfg, rpcRef) {
  const cmd = args._[0];
  const need = (k) => { if (!args[k] || args[k] === true) throw new Error(`missing --${k}`); return args[k]; };
  const rpc = async () => (rpcRef.c ??= await RpcClient.connect(cfg));
  switch (cmd) {
    case "login": {
      const c = { ...cfg, baseUrl: need("url") };
      if (args.token) c.token = args.token;
      else { const r = await exchangePairingCode(c.baseUrl, need("pair")); c.token = r.access_token; console.log(`scope: ${r.scope}, expires_in: ${r.expires_in}s`); }
      saveConfig(c); await wsTicket(c); console.log(`ok, saved ${CONFIG_PATH}`); return;
    }
    case "frames": {
      const create = buildThreadCreate(cfg, { projectId: need("project"), title: need("title") });
      for (const p of [create, buildTurnStart(cfg, { threadId: create.threadId, text: need("prompt") })])
        console.log(JSON.stringify({ _tag: "Request", id: "0", tag: "orchestration.dispatchCommand", payload: p, headers: [] }));
      return;
    }
    case "projects": {
      const r = await new Promise((res, rej) => {
        const h = (rpcRef.c ? Promise.resolve(rpcRef.c) : rpc()).then((c) => {
          const hh = c.call("orchestration.subscribeShell", {}, (item) => { if (item.snapshot || item.kind === "snapshot") { hh.interrupt(); res(item.snapshot ?? item); } });
          hh.promise.catch(rej);
        }, rej);
        setTimeout(() => rej(new Error("timeout")), 15000);
      });
      for (const p of r.projects ?? []) console.log(`${p.id}\t${p.title}\t${p.workspaceRoot ?? ""}`);
      return;
    }
    case "create": {
      const c = await rpc();
      const create = buildThreadCreate(cfg, { projectId: need("project"), title: need("title") });
      await c.request("orchestration.dispatchCommand", create);
      await c.request("orchestration.dispatchCommand", buildTurnStart(cfg, { threadId: create.threadId, text: need("prompt") }));
      console.log(`created thread ${create.threadId}`);
      if (args.wait) console.log(`turn: ${await waitForTurn(c, create.threadId, Number(args.wait) * 1000)}`);
      return;
    }
    case "send": {
      const c = await rpc();
      const threadId = need("thread");
      await c.request("orchestration.dispatchCommand", buildTurnStart(cfg, { threadId, text: need("prompt") }));
      console.log(`sent to ${threadId}`);
      if (args.wait) console.log(`turn: ${await waitForTurn(c, threadId, Number(args.wait) * 1000)}`);
      return;
    }
    case "status": printStatus(await threadSnapshot(await rpc(), need("thread"))); return;
    case "read": printMessages(await threadSnapshot(await rpc(), need("thread")), !!args.all); return;
    case "batch": {
      for (const line of readFileSync(need("file"), "utf8").split("\n").filter((l) => l.trim())) {
        const j = JSON.parse(line); const sub = { _: [j.cmd] };
        for (const [k, v] of Object.entries(j)) if (k !== "cmd") sub[k] = String(v);
        console.log(`> ${j.cmd}`); await run(sub, cfg, rpcRef);
      }
      return;
    }
    default: console.log(USAGE); if (cmd && cmd !== "help") process.exitCode = 2;
  }
}

if (import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith("t3-alpha.mjs")) {
  const args = parseArgs(process.argv.slice(2));
  const rpcRef = {};
  run(args, loadConfig(), rpcRef)
    .catch((e) => { console.error(`error: ${e.message}`); process.exitCode = 1; })
    .finally(() => rpcRef.c?.close());
}
