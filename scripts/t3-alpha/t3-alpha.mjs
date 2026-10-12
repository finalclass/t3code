#!/usr/bin/env node
// t3-alpha: minimal console client for a T3 Code server (wire format of v0.0.45).
// No dependencies; needs Node >= 22 (global fetch + WebSocket).
// Wire: Effect RPC over WebSocket, JSON serialization (apps/server/src/ws.ts RpcSerialization.layerJson).
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";

const CONFIG_PATH = process.env.T3_ALPHA_CONFIG || join(homedir(), ".config", "t3-alpha.json");

function loadConfig() {
  const c = existsSync(CONFIG_PATH) ? JSON.parse(readFileSync(CONFIG_PATH, "utf8")) : {};
  if (process.env.T3_ALPHA_URL) c.baseUrl = process.env.T3_ALPHA_URL;
  if (process.env.T3_ALPHA_TOKEN) c.token = process.env.T3_ALPHA_TOKEN;
  c.baseUrl ||= c.url || "http://127.0.0.1:5773";
  return c;
}
function saveConfig(c) {
  mkdirSync(dirname(CONFIG_PATH), { recursive: true });
  writeFileSync(CONFIG_PATH, JSON.stringify(c, null, 2) + "\n", { mode: 0o600 });
}

// ---- HTTP auth ----
async function exchangePairingCode(baseUrl, code) {
  const body = new URLSearchParams({
    grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
    subject_token: code,
    subject_token_type: "urn:t3:params:oauth:token-type:environment-bootstrap",
    requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
    client_label: "t3-alpha CLI",
    client_device_type: "desktop",
  });
  const r = await fetch(new URL("/oauth/token", baseUrl), {
    method: "POST", headers: { "content-type": "application/x-www-form-urlencoded" }, body,
  });
  const t = await r.text();
  if (!r.ok) throw new Error(`token exchange HTTP ${r.status}: ${t}`);
  return JSON.parse(t);
}
async function wsTicket(cfg) {
  if (!cfg.token) throw new Error(`no token in ${CONFIG_PATH}; run login first`);
  const r = await fetch(new URL("/api/auth/websocket-ticket", cfg.baseUrl), {
    method: "POST", headers: { authorization: `Bearer ${cfg.token}` },
  });
  if (!r.ok) throw new Error(`websocket-ticket HTTP ${r.status}`);
  return (await r.json()).ticket;
}

// ---- Effect RPC client (JSON frames) ----
class RpcClient {
  constructor(ws) {
    this.ws = ws; this.next = 0; this.pending = new Map(); this.closing = false;
    ws.addEventListener("message", (ev) => this.onMessage(String(ev.data)));
    ws.addEventListener("close", () => {
      const err = new Error("socket closed by server");
      for (const p of this.pending.values()) this.closing ? p.resolve(undefined) : p.reject(err);
      this.pending.clear();
    });
    this.ping = setInterval(() => ws.readyState === 1 && this.send({ _tag: "Ping" }), 10000);
  }
  static async connect(cfg) {
    const u = new URL("/ws", cfg.baseUrl);
    u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
    u.searchParams.set("wsTicket", await wsTicket(cfg));
    u.searchParams.set("clientSurface", "web");
    u.searchParams.set("clientDeviceType", "desktop");
    u.searchParams.set("orchestrationProtocol", "1");
    const ws = new WebSocket(u);
    await new Promise((res, rej) => {
      ws.addEventListener("open", res, { once: true });
      ws.addEventListener("error", () => rej(new Error("WebSocket connect failed")), { once: true });
    });
    return new RpcClient(ws);
  }
  close() {
    this.closing = true; clearInterval(this.ping);
    for (const p of this.pending.values()) p.resolve(undefined);
    this.pending.clear();
    try { this.ws.close(1000); } catch {}
  }
  send(o) { if (this.ws.readyState === 1) this.ws.send(JSON.stringify(o)); }
  onMessage(raw) {
    let msgs; try { msgs = JSON.parse(raw); } catch { return; }
    for (const m of Array.isArray(msgs) ? msgs : [msgs]) {
      const id = String(m.requestId);
      const p = this.pending.get(id);
      if (m._tag === "Chunk") {
        this.send({ _tag: "Ack", requestId: id });
        if (p) for (const v of m.values) p.onChunk?.(v);
      } else if (m._tag === "Exit" && p) {
        this.pending.delete(id);
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
    const interrupt = () => {
      const p = this.pending.get(id);
      if (!p) return;
      this.pending.delete(id); p.resolve(undefined);
      this.send({ _tag: "Interrupt", requestId: id, interruptors: [] });
    };
    return { promise, interrupt };
  }
  request(tag, payload) { return this.call(tag, payload).promise; }
  // First `snapshot` item of a subscription stream, then unsubscribe.
  firstSnapshot(tag, payload, ms = 20000) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { h.interrupt(); reject(new Error(`timeout waiting for ${tag} snapshot`)); }, ms);
      const h = this.call(tag, payload, (item) => {
        if (item?.kind === "snapshot") { clearTimeout(timer); h.interrupt(); resolve(item.snapshot); }
      });
      h.promise.then(
        () => { clearTimeout(timer); reject(new Error(`${tag} ended without snapshot`)); },
        (e) => { clearTimeout(timer); reject(e); },
      );
    });
  }
}

// ---- helpers ----
const now = () => new Date().toISOString();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const threadSnapshot = (rpc, threadId) => rpc.firstSnapshot("orchestration.subscribeThread", { threadId });
const shellSnapshot = (rpc) => rpc.firstSnapshot("orchestration.subscribeShell", {});

function latestReply(thread) {
  const msgs = thread.messages ?? [];
  const id = thread.latestTurn?.assistantMessageId;
  const m = (id && msgs.find((x) => x.id === id)) || [...msgs].reverse().find((x) => x.role === "assistant");
  return m ? m.text.trim() : "";
}
function printStatus(thread) {
  const lt = thread.latestTurn;
  const settled = thread.settledOverride === "settled" || (thread.settledAt != null && thread.settledOverride !== "active");
  console.log(`thread ${thread.id} "${thread.title}" turn=${lt ? lt.state : "none"} settled=${settled ? `yes (${thread.settledAt ?? "override"})` : "no"}`);
  const reply = latestReply(thread);
  console.log(reply ? `reply:\n${reply}` : "reply: (none yet)");
}

// Thread defaults come from the server: project's defaultModelSelection, else the
// most recently updated thread in that project. Nothing is hardcoded.
function resolveThreadDefaults(shell, projectId) {
  const project = shell.projects.find((p) => p.id === projectId);
  if (!project) throw new Error(`project ${projectId} not found on server`);
  const recent = shell.threads
    .filter((t) => t.projectId === projectId)
    .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)))[0];
  const modelSelection = project.defaultModelSelection ?? recent?.modelSelection;
  if (!modelSelection) throw new Error(`project ${project.title} has no default model and no threads to copy one from`);
  return {
    modelSelection,
    runtimeMode: recent?.runtimeMode ?? "full-access",
    interactionMode: recent?.interactionMode ?? "default",
  };
}

async function waitForTurn(rpc, threadId, previousTurnId, deadline) {
  while (Date.now() < deadline) {
    const { thread } = await threadSnapshot(rpc, threadId);
    const lt = thread.latestTurn;
    if (lt && lt.turnId !== previousTurnId && lt.state !== "running") return thread;
    await sleep(3000);
  }
  throw new Error("timeout waiting for turn to finish");
}

// ---- CLI ----
function parseArgs(argv) {
  const o = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      o[k] = argv[i + 1] === undefined || argv[i + 1].startsWith("--") ? true : argv[++i];
    } else o._.push(a);
  }
  return o;
}
const USAGE = `t3-alpha <command> [options]
  login  --url <http://127.0.0.1:5773> (--token <bearer> | --pair <pairing-code>)
  projects
  create --project <id> --title <t> --prompt <p> [--timeout <sec>=900] [--no-wait]
  send   --thread <id> --prompt <p> [--timeout <sec>=900] [--no-wait]
  status --thread <id>        (alias: read)
  settle --thread <id>
  batch  --file <path>        JSONL: {"cmd":"create","project":"...","title":"...","prompt":"..."}
Config: ${CONFIG_PATH} (env T3_ALPHA_URL / T3_ALPHA_TOKEN override)`;

async function run(args, cfg, ctx) {
  const cmd = args._[0];
  const need = (k) => { if (!args[k] || args[k] === true) throw new Error(`missing --${k}`); return args[k]; };
  const rpc = async () => (ctx.rpc ??= await RpcClient.connect(cfg));
  const deadline = Date.now() + Number(args.timeout ?? 900) * 1000;

  async function sendPrompt(c, threadId, text, defaults, previousTurnId) {
    await c.request("orchestration.dispatchCommand", {
      type: "thread.turn.start", commandId: randomUUID(), threadId,
      message: { messageId: randomUUID(), role: "user", text, attachments: [] },
      runtimeMode: defaults.runtimeMode, interactionMode: defaults.interactionMode, createdAt: now(),
    });
    if (args["no-wait"]) { console.log(`ok: prompt sent to thread ${threadId}`); return; }
    const thread = await waitForTurn(c, threadId, previousTurnId, deadline);
    const reply = latestReply(thread);
    console.log(`ok: thread ${threadId} turn=${thread.latestTurn.state}`);
    if (reply) console.log(reply);
    if (thread.latestTurn.state !== "completed") ctx.exitCode = 1;
  }

  switch (cmd) {
    case "login": {
      const c = { ...cfg, baseUrl: need("url") };
      if (args.token) c.token = args.token;
      else c.token = (await exchangePairingCode(c.baseUrl, need("pair"))).access_token;
      await wsTicket(c); saveConfig(c); console.log(`ok: saved ${CONFIG_PATH}`); return;
    }
    case "projects": {
      const s = await shellSnapshot(await rpc());
      for (const p of s.projects) console.log(`${p.id}\t${p.title}\t${p.workspaceRoot}`);
      return;
    }
    case "create": {
      const c = await rpc();
      const projectId = need("project"), title = need("title"), prompt = need("prompt");
      const defaults = resolveThreadDefaults(await shellSnapshot(c), projectId);
      const threadId = randomUUID();
      await c.request("orchestration.dispatchCommand", {
        type: "thread.create", commandId: randomUUID(), threadId, projectId, title,
        modelSelection: defaults.modelSelection, runtimeMode: defaults.runtimeMode,
        interactionMode: defaults.interactionMode, branch: null, worktreePath: null, createdAt: now(),
      });
      await sendPrompt(c, threadId, prompt, defaults, null);
      return;
    }
    case "send": {
      const c = await rpc();
      const threadId = need("thread");
      const { thread } = await threadSnapshot(c, threadId);
      await sendPrompt(c, threadId, need("prompt"),
        { runtimeMode: thread.runtimeMode, interactionMode: thread.interactionMode }, thread.latestTurn?.turnId ?? null);
      return;
    }
    case "settle": {
      const threadId = need("thread");
      const r = await (await rpc()).request("orchestration.dispatchCommand", { type: "thread.settle", commandId: randomUUID(), threadId });
      console.log(`ok: settled thread ${threadId} (sequence ${r?.sequence})`);
      return;
    }
    case "status": case "read": {
      printStatus((await threadSnapshot(await rpc(), need("thread"))).thread);
      return;
    }
    case "batch": {
      for (const line of readFileSync(need("file"), "utf8").split("\n").filter((l) => l.trim())) {
        const j = JSON.parse(line); const sub = { _: [j.cmd] };
        for (const [k, v] of Object.entries(j)) if (k !== "cmd") sub[k] = v === true ? true : String(v);
        console.log(`> ${j.cmd}`); await run(sub, cfg, ctx);
      }
      return;
    }
    default:
      console.log(USAGE);
      if (cmd && cmd !== "help") ctx.exitCode = 2;
  }
}

const args = parseArgs(process.argv.slice(2));
const ctx = { exitCode: 0 };
const hardTimeout = setTimeout(() => {
  console.error("error: overall timeout"); process.exit(1);
}, (Number(args.timeout ?? 900) + 60) * 1000);
hardTimeout.unref();
try {
  await run(args, loadConfig(), ctx);
} catch (e) {
  console.error(`error: ${e.message}`); ctx.exitCode = 1;
} finally {
  ctx.rpc?.close();
}
process.exit(ctx.exitCode);
