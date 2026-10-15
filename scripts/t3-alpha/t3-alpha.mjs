#!/usr/bin/env node
// t3-alpha: console client for a T3 Code server (wire format of v0.0.45).
// No dependencies; needs Node >= 22 (global fetch + WebSocket).
// Wire: Effect RPC over WebSocket, JSON serialization (apps/server/src/ws.ts RpcSerialization.layerJson).
// Every command opens one socket, sends one request, waits for Exit, then closes.
// Model, runtime and interaction mode change only when the command says so.
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";

const CONFIG_PATH = process.env.T3_ALPHA_CONFIG || join(homedir(), ".config", "t3-alpha.json");
const RPC_TIMEOUT_MS = 30_000;
const SNAPSHOT_TIMEOUT_MS = 45_000;
const CONNECT_TIMEOUT_MS = 15_000;
const RUNTIME_MODES = ["approval-required", "auto-accept-edits", "auto", "full-access"];
const INTERACTION_MODES = ["default", "plan"];
const APPROVAL_DECISIONS = ["accept", "acceptForSession", "acceptAlways", "decline", "cancel"];
const PR_SOURCES = ["manual", "created", "agent", "stack", "stack-dismissed"];
const ENV_MODES = ["local", "worktree"];

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
    ws.addEventListener("error", () => {
      if (this.closing) return;
      const err = new Error("WebSocket error");
      for (const p of this.pending.values()) p.reject(err);
      this.pending.clear();
    });
    this.ping = setInterval(() => ws.readyState === 1 && this.send({ _tag: "Ping" }), 10000);
    this.ping.unref?.();
  }
  static async connect(cfg) {
    const u = new URL("/ws", cfg.baseUrl);
    u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
    u.searchParams.set("wsTicket", await wsTicket(cfg));
    u.searchParams.set("clientSurface", "cli");
    u.searchParams.set("clientAppVersion", "t3-alpha");
    u.searchParams.set("clientDeviceType", "desktop");
    u.searchParams.set("orchestrationProtocol", "1");
    const ws = new WebSocket(u);
    await new Promise((res, rej) => {
      const timer = setTimeout(() => rej(new Error("WebSocket connect timeout")), CONNECT_TIMEOUT_MS);
      ws.addEventListener("open", () => { clearTimeout(timer); res(); }, { once: true });
      ws.addEventListener("error", () => { clearTimeout(timer); rej(new Error("WebSocket connect failed")); }, { once: true });
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
  request(tag, payload, ms = RPC_TIMEOUT_MS) {
    const { promise, interrupt } = this.call(tag, payload);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`timeout waiting for ${tag}`));
        interrupt();
      }, ms);
      promise.then(
        (value) => { clearTimeout(timer); resolve(value); },
        (error) => { clearTimeout(timer); reject(error); },
      );
    });
  }
  // First `snapshot` item of a subscription stream, then unsubscribe.
  firstSnapshot(tag, payload, ms = SNAPSHOT_TIMEOUT_MS) {
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
const archivedThreads = async (rpc) => (await rpc.request("orchestration.getArchivedShellSnapshot", {}))?.threads ?? [];
function isMissingThread(error) {
  return /not found/i.test(String(error?.message ?? ""));
}

function latestReply(thread) {
  const msgs = thread.messages ?? [];
  const id = thread.latestTurn?.assistantMessageId;
  const m = (id && msgs.find((x) => x.id === id)) || [...msgs].reverse().find((x) => x.role === "assistant");
  return m ? m.text.trim() : "";
}
function isSettled(thread) {
  return thread.settledOverride === "settled" || (thread.settledAt != null && thread.settledOverride !== "active");
}
function isSnoozed(thread) {
  if (!thread.snoozedUntil) return false;
  const at = Date.parse(thread.snoozedUntil);
  return Number.isFinite(at) && at > Date.now();
}
function printStatus(thread) {
  const lt = thread.latestTurn;
  const settled = isSettled(thread);
  console.log(`thread ${thread.id} "${thread.title}" turn=${lt ? lt.state : "none"} settled=${settled ? `yes (${thread.settledAt ?? "override"})` : "no"}`);
  const reply = latestReply(thread);
  console.log(reply ? `reply:\n${reply}` : "reply: (none yet)");
}
function optionList(options) {
  if (Array.isArray(options)) return options.filter((o) => o && o.id).map((o) => ({ id: String(o.id), value: o.value }));
  if (options && typeof options === "object") {
    return Object.entries(options).filter(([, v]) => v !== undefined && v !== null).map(([id, value]) => ({ id, value }));
  }
  return [];
}
function formatOptions(options) {
  const list = optionList(options);
  return list.length ? list.map((o) => `${o.id}=${String(o.value)}`).join(",") : "-";
}
function pendingFromActivities(activities = []) {
  const approvals = new Map();
  const inputs = new Map();
  const closedApprovals = new Set();
  const closedInputs = new Set();
  for (const activity of activities) {
    const payload = activity?.payload;
    const requestId = payload && typeof payload.requestId === "string" ? payload.requestId : null;
    if (!requestId) continue;
    if (activity.kind === "approval.requested" && !closedApprovals.has(requestId)) {
      if (payload.requestType === "tool_user_input" || payload.requestType === "auth_tokens_refresh") continue;
      approvals.set(requestId, {
        requestId,
        requestKind: payload.requestKind ?? payload.requestType ?? null,
        detail: typeof payload.detail === "string" ? payload.detail : null,
        options: Array.isArray(payload.options) ? payload.options : [],
      });
    } else if (activity.kind === "approval.resolved") {
      closedApprovals.add(requestId);
      approvals.delete(requestId);
    } else if (activity.kind === "user-input.requested" && !closedInputs.has(requestId)) {
      inputs.set(requestId, {
        requestId,
        dismissible: payload.responseMode === "message",
        questions: Array.isArray(payload.questions) ? payload.questions : [],
      });
    } else if (activity.kind === "user-input.resolved") {
      closedInputs.add(requestId);
      inputs.delete(requestId);
    }
  }
  return { approvals: [...approvals.values()], userInputs: [...inputs.values()] };
}
function formatTranscript(thread) {
  const model = thread.modelSelection ?? {};
  const lines = [
    `model: ${model.model ?? "-"} instance=${model.instanceId ?? "-"} options=${formatOptions(model.options)}`,
    `runtime: ${thread.runtimeMode ?? "-"}`,
    `interaction: ${thread.interactionMode ?? "-"}`,
    `session: ${thread.session?.status ?? "none"} activeTurn=${thread.session?.activeTurnId ?? "-"}`,
    `branch: ${thread.branch ?? "-"}`,
    `worktree: ${thread.worktreePath ?? "-"}`,
    `archived: ${thread.archivedAt ?? "no"} pinned: ${thread.pinnedAt ?? "no"} snoozedUntil: ${thread.snoozedUntil ?? "no"}`,
    `autoSettle: ${thread.autoSettleDisabledAt ? "off" : "on"}`,
  ];
  const messages = thread.messages ?? [];
  lines.push(`messages: ${messages.length}`);
  for (const message of messages) {
    lines.push(`--- ${message.role} ${message.id} turn=${message.turnId ?? "-"} streaming=${message.streaming ? "yes" : "no"}`);
    lines.push(message.text ?? "");
  }
  const pending = pendingFromActivities(thread.activities);
  if (pending.approvals.length === 0) lines.push("pending-approval: none");
  for (const approval of pending.approvals) {
    lines.push(`pending-approval: ${approval.requestId} kind=${approval.requestKind ?? "-"} detail=${approval.detail ?? "-"}`);
  }
  if (pending.userInputs.length === 0) lines.push("pending-input: none");
  for (const input of pending.userInputs) {
    lines.push(`pending-input: ${input.requestId} dismissible=${input.dismissible ? "yes" : "no"}`);
    for (const question of input.questions) {
      const label = typeof question === "object" && question ? (question.question ?? question.header ?? question.id) : String(question);
      const id = typeof question === "object" && question ? question.id : "?";
      lines.push(`  question ${id}: ${label}`);
    }
  }
  return lines.join("\n");
}
function threadView(thread) {
  const pending = pendingFromActivities(thread.activities);
  return {
    id: thread.id,
    projectId: thread.projectId,
    title: thread.title,
    turn: thread.latestTurn ?? null,
    settled: isSettled(thread),
    settledAt: thread.settledAt ?? null,
    settledOverride: thread.settledOverride ?? null,
    archivedAt: thread.archivedAt ?? null,
    pinnedAt: thread.pinnedAt ?? null,
    snoozedUntil: thread.snoozedUntil ?? null,
    autoSettleDisabledAt: thread.autoSettleDisabledAt ?? null,
    modelSelection: thread.modelSelection ?? null,
    runtimeMode: thread.runtimeMode ?? null,
    interactionMode: thread.interactionMode ?? null,
    branch: thread.branch ?? null,
    worktreePath: thread.worktreePath ?? null,
    session: thread.session ?? null,
    messages: (thread.messages ?? []).map((message) => ({
      id: message.id,
      role: message.role,
      text: message.text,
      turnId: message.turnId ?? null,
      streaming: message.streaming === true,
      createdAt: message.createdAt ?? null,
    })),
    pendingApprovals: pending.approvals,
    pendingUserInputs: pending.userInputs,
    reply: latestReply(thread),
  };
}

// Thread defaults come from the server: project's defaultModelSelection, else the
// most recently updated thread in that project. Nothing is hardcoded.
function resolveThreadDefaults(shell, projectId) {
  const project = shell.projects.find((p) => p.id === projectId);
  if (!project) throw new Error(`project ${projectId} not found on server`);
  const recent = shell.threads
    .filter((t) => t.projectId === projectId && !t.deletedAt)
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

function parseArgs(argv) {
  const o = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const next = argv[i + 1];
      const val = next === undefined || next.startsWith("--") ? true : argv[++i];
      if (k === "option" || k === "answer") {
        const list = Array.isArray(o[k]) ? o[k] : [];
        list.push(val);
        o[k] = list;
      } else o[k] = val;
    } else o._.push(a);
  }
  return o;
}
function opt(args, key) {
  const value = args[key];
  if (value === undefined || value === true) return undefined;
  return String(value);
}
function need(args, key) {
  const value = opt(args, key);
  if (value === undefined) throw new Error(`missing --${key}`);
  return value;
}
function parseBool(value, flag) {
  if (value === true || value === "true" || value === "on" || value === "yes") return true;
  if (value === "false" || value === "off" || value === "no") return false;
  throw new Error(`--${flag} expects true or false`);
}
function oneOf(value, allowed, flag) {
  if (!allowed.includes(value)) throw new Error(`--${flag} must be one of: ${allowed.join(", ")}`);
  return value;
}
function parseJson(text, flag) {
  try { return JSON.parse(text); }
  catch { throw new Error(`--${flag} is not valid JSON`); }
}
function optionValue(raw) {
  if (raw === "true") return true;
  if (raw === "false") return false;
  return String(raw);
}
function applyOption(options, id, value) {
  const next = options.filter((option) => option.id !== id);
  next.push({ id, value });
  return next;
}
const REASONING_OPTION_IDS = ["reasoningEffort", "effort"];
function reasoningOptionId(options) {
  return REASONING_OPTION_IDS.find((id) => options.some((option) => option.id === id));
}
async function lookupReasoningOptionId(rpc, instanceId, model) {
  const config = await rpc.request("server.getConfig", {});
  const provider = (config.providers ?? []).find((item) => item.instanceId === instanceId);
  const found = (provider?.models ?? []).find((item) => item.slug === model);
  const descriptor = (found?.capabilities?.optionDescriptors ?? []).find((item) => REASONING_OPTION_IDS.includes(item.id));
  if (!descriptor) throw new Error(`model ${model} has no reasoningEffort or effort option; pass --option id=value`);
  return descriptor.id;
}
async function resolveReasoningId(rpc, current, args) {
  if (opt(args, "effort") === undefined) return undefined;
  const model = opt(args, "model") ?? current?.model;
  let reasoningId;
  if (model && model === current?.model) reasoningId = reasoningOptionId(optionList(current?.options));
  if (!reasoningId) reasoningId = await lookupReasoningOptionId(rpc, opt(args, "instance") ?? current?.instanceId, model);
  return reasoningId;
}
function modelSelectionFrom(current, args, reasoningId) {
  const model = opt(args, "model") ?? current?.model;
  const instanceId = opt(args, "instance") ?? current?.instanceId;
  if (!model || !instanceId) throw new Error("model change needs --model and --instance, or an existing selection on the thread");
  let options = optionList(current?.options);
  if (args["clear-options"] === true) options = [];
  const effort = opt(args, "effort");
  if (effort !== undefined) options = applyOption(options, reasoningId ?? reasoningOptionId(options) ?? "reasoningEffort", effort);
  for (const raw of args.option ?? []) {
    const text = String(raw);
    const eq = text.indexOf("=");
    if (eq <= 0) throw new Error("--option expects id=value");
    options = applyOption(options, text.slice(0, eq), optionValue(text.slice(eq + 1)));
  }
  const selection = { instanceId, model };
  if (options.length > 0 || args["clear-options"] === true) selection.options = options;
  return selection;
}
function hasModelArgs(args) {
  return opt(args, "model") !== undefined || opt(args, "instance") !== undefined
    || opt(args, "effort") !== undefined || (args.option?.length ?? 0) > 0 || args["clear-options"] === true;
}
function emit(args, text, data) {
  if (args.json === true) console.log(JSON.stringify(data, null, 2));
  else console.log(text);
}
function sequenceOf(result) {
  return result && typeof result === "object" ? result.sequence : undefined;
}
async function dispatch(rpc, command) {
  return rpc.request("orchestration.dispatchCommand", { commandId: randomUUID(), ...command });
}
async function loadThread(rpc, threadId) {
  const snapshot = await threadSnapshot(rpc, threadId);
  if (!snapshot?.thread) throw new Error(`thread ${threadId} not found`);
  return snapshot.thread;
}
function selectThreads(threads, args) {
  const project = opt(args, "project");
  const shelves = [];
  if (args.unsettled === true) shelves.push("unsettled");
  if (args.settled === true) shelves.push("settled");
  if (args.archived === true) shelves.push("archived");
  if (args.snoozed === true) shelves.push("snoozed");
  return (threads ?? []).filter((thread) => {
    if (thread.deletedAt) return false;
    if (project && thread.projectId !== project) return false;
    const archived = thread.archivedAt != null;
    const settled = isSettled(thread);
    const snoozed = isSnoozed(thread);
    const pinned = thread.pinnedAt != null;
    if (args.pinned === true && !pinned) return false;
    if (shelves.length === 0) return args.all === true || !archived;
    return shelves.some((shelf) => {
      if (shelf === "archived") return archived;
      if (shelf === "settled") return settled && !archived;
      if (shelf === "snoozed") return snoozed && !archived;
      return !settled && !archived && !snoozed;
    });
  });
}
function threadRow(thread) {
  const flags = [];
  if (thread.archivedAt) flags.push("archived");
  else if (isSnoozed(thread)) flags.push("snoozed");
  else if (isSettled(thread)) flags.push("settled");
  else flags.push("unsettled");
  if (thread.pinnedAt) flags.push("pinned");
  if (thread.hasPendingApprovals) flags.push("approval");
  if (thread.hasPendingUserInput) flags.push("input");
  return {
    id: thread.id,
    projectId: thread.projectId,
    title: thread.title,
    turn: thread.latestTurn?.state ?? "none",
    flags,
    updatedAt: thread.updatedAt ?? null,
    model: thread.modelSelection?.model ?? null,
    modelSelection: thread.modelSelection ?? null,
    runtimeMode: thread.runtimeMode ?? null,
    interactionMode: thread.interactionMode ?? null,
  };
}

const HELP = {
  login: `login --url <base> (--token <bearer> | --pair <pairing-code>)
  Exchange a pairing code, or store a bearer token, then check the websocket ticket.
  Config: ${CONFIG_PATH}`,
  projects: `projects [--json]
  List projects as id, title, workspaceRoot.`,
  models: `models [--json]
  List provider instances and their model slugs, including effort/option choices.
  Does not change any thread.`,
  threads: `threads [--project <id>] [--unsettled] [--settled] [--archived] [--snoozed] [--pinned] [--all] [--json]
  List threads. With no shelf flag, archived threads are hidden. Shelf flags combine as a union.
  --pinned and --project narrow the result. --unsettled is the inbox: not settled, not snoozed, not archived.
  The live shell omits archived threads. --archived and --all read orchestration.getArchivedShellSnapshot.`,
  create: `create --project <id> --title <title> --prompt <text> [--timeout <sec>=900] [--no-wait]
         [--model <slug>] [--instance <id>] [--effort <level>] [--option id=value] [--runtime <mode>] [--interaction <mode>] [--json]
  Create a thread and send the prompt. The model, runtime and interaction mode come from the server
  (project default, else the latest thread in the project) unless a flag sets them explicitly.`,
  send: `send --thread <id> --prompt <text> [--timeout <sec>=900] [--no-wait]
      [--model <slug>] [--instance <id>] [--effort <level>] [--option id=value] [--runtime <mode>] [--interaction <mode>] [--json]
  Append a prompt to an existing thread. Runtime and interaction mode stay on the thread unless a flag sets them.
  A model is sent only when --model, --instance, --effort, --option, or --clear-options is present.`,
  read: `read --thread <id> [--json]
  Alias: status. Prints the turn summary and latest reply, then the transcript, model, and pending requests.
  An archived thread has no detail subscription. The command then prints the archived shell and says messages need unarchive.`,
  status: `status --thread <id> [--json]
  Alias of read.`,
  settle: `settle --thread <id> [--json]
  Mark the thread settled. A thread that is already settled is left unchanged.`,
  unsettle: `unsettle --thread <id> [--json]
  Return a settled thread to the inbox (reason=user).`,
  archive: `archive --thread <id> [--json]`,
  unarchive: `unarchive --thread <id> [--json]
  Archived threads are absent from subscribeThread. This command sends thread.unarchive without that snapshot.`,
  pin: `pin --thread <id> [--order-key <key>] [--json]`,
  unpin: `unpin --thread <id> [--json]`,
  "reorder-pin": `reorder-pin --thread <id> --order-key <key> [--json]
  Set the pinned-list sort key. Keys sort lexicographically.`,
  "reorder-active": `reorder-active --thread <id> --order-key <key> [--json]`,
  snooze: `snooze --thread <id> (--until <ISO-8601> | --minutes <n>) [--json]`,
  unsnooze: `unsnooze --thread <id> [--json]
  Wake a snoozed thread (reason=user).`,
  interrupt: `interrupt --thread <id> [--turn <turnId>] [--json]
  Interrupt the running turn (thread.turn.interrupt).`,
  model: `model --thread <id> [--model <slug>] [--instance <id>] [--effort <level>] [--option id=value]... [--clear-options] [--json]
  thread.meta.update with modelSelection. At least one model flag is required.
  --effort sets the model's reasoning option. Codex and Grok use reasoningEffort; Claude uses effort.
  The id already stored on the thread wins; otherwise the server catalog decides.
  Other provider options use --option id=value (true/false becomes a boolean).
  Omitted fields stay as they are on the thread.`,
  runtime: `runtime --thread <id> --mode <${RUNTIME_MODES.join("|")}> [--json]`,
  interaction: `interaction --thread <id> --mode <${INTERACTION_MODES.join("|")}> [--json]`,
  approve: `approve --thread <id> --request <requestId> --decision <${APPROVAL_DECISIONS.join("|")}> [--json]`,
  answer: `answer --thread <id> --request <requestId> (--answers <json> | --answer <questionId>=<text>)... [--json]
  Reply to a pending user-input request. --answers is a JSON object keyed by question id.
  Repeated --answer sets string values and overrides the same id from --answers.`,
  dismiss: `dismiss --thread <id> --request <requestId> [--json]
  Close a dismissible user-input request without answering it.`,
  rename: `rename --thread <id> --title <title> [--json]`,
  retitle: `retitle --thread <id> [--json]
  Ask the server to regenerate the title.`,
  branch: `branch --thread <id> (--name <branch> | --clear) [--expected <current>] [--json]`,
  worktree: `worktree --thread <id> (--path <path> | --clear) [--json]`,
  autosettle: `autosettle --thread <id> (--on | --off) [--json]`,
  stop: `stop --thread <id> [--only-if-settled] [--json]
  Stop the provider session (thread.session.stop).`,
  revert: `revert --thread <id> --turn-count <n> [--json]
  Restore files to the checkpoint at that turn count (thread.checkpoint.revert).`,
  "revert-chat": `revert-chat --thread <id> --turn-count <n> [--json]
  Rewind the conversation without restoring files (thread.conversation.revert).`,
  "link-pr": `link-pr --thread <id> --host <host> --repository <owner/name> --number <n> --url <url> [--source <${PR_SOURCES.join("|")}>] [--json]
  Default source is manual.`,
  "unlink-pr": `unlink-pr --thread <id> --host <host> --repository <owner/name> --number <n> [--json]`,
  forget: `forget --thread <id> [--json]
  Delete the thread (thread.delete). delete is the same command.`,
  delete: `delete --thread <id> [--json]
  Alias of forget.`,
  "project-create": `project-create --title <title> --workspace <path> [--id <id>] [--create-root] [--json]
  Create a project. --id defaults to a new UUID. Does not set a default model.`,
  "project-update": `project-update --project <id> [--title <title>] [--workspace <path>] [--auto-pull true|false]
                 [--env local|worktree] [--clear-env] [--favicon <path>] [--clear-favicon]
                 [--icon-json <json>] [--clear-icon] [--scripts-json <json>]
                 [--default-model-json <json>] [--clear-default-model] [--json]
  Patch project metadata. Only the flags you pass are sent. A default model is sent only with
  --default-model-json or cleared with --clear-default-model.`,
  "project-delete": `project-delete --project <id> [--force] [--json]`,
  search: `search --query <text> [--limit <1-50>] [--json]
  orchestration.searchThreads. The query must be 2 to 200 characters.`,
  diff: `diff --thread <id> --to <turnCount> [--from <turnCount>] [--ignore-whitespace] [--json]
  --from and --to read orchestration.getTurnDiff. --to alone reads orchestration.getFullThreadDiff.`,
  "workflow-script": `workflow-script --thread <id> --path <absolute.js> [--json]
  Read a workflow script the server already stored (orchestration.getWorkflowScript).`,
  batch: `batch --file <path>
  Run one command per JSONL line: {"cmd":"settle","thread":"..."}
  Keys other than cmd become flags. true stays a boolean flag.`,
};

const USAGE = `t3-alpha <command> [--json] [--help]
${Object.keys(HELP).join("\n")}

Global: --json prints one JSON document instead of text. --timeout <sec> bounds turn waits (default 900).
Config: ${CONFIG_PATH} (env T3_ALPHA_URL / T3_ALPHA_TOKEN override).
t3-alpha help <command> prints that command. The CLI never picks a model on its own.`;

function printHelp(topic) {
  if (!topic) {
    console.log(USAGE);
    return true;
  }
  const text = HELP[topic];
  if (!text) {
    console.log(USAGE);
    console.error(`error: unknown command ${topic}`);
    return false;
  }
  console.log(text);
  return true;
}

async function run(args, cfg, ctx) {
  const cmd = args._[0];
  const rpc = async () => (ctx.rpc ??= await RpcClient.connect(cfg));
  const deadline = Date.now() + Number(args.timeout ?? 900) * 1000;

  async function sendPrompt(c, threadId, text, defaults, previousTurnId) {
    const message = {
      type: "thread.turn.start", threadId,
      message: { messageId: randomUUID(), role: "user", text, attachments: [] },
      runtimeMode: defaults.runtimeMode, interactionMode: defaults.interactionMode, createdAt: now(),
    };
    if (hasModelArgs(args)) {
      const reasoningId = await resolveReasoningId(c, defaults.modelSelection, args);
      message.modelSelection = modelSelectionFrom(defaults.modelSelection, args, reasoningId);
    }
    await dispatch(c, message);
    if (args["no-wait"]) {
      emit(args, `ok: prompt sent to thread ${threadId}`, { ok: true, threadId, sent: true });
      return;
    }
    const thread = await waitForTurn(c, threadId, previousTurnId, deadline);
    const reply = latestReply(thread);
    const state = thread.latestTurn.state;
    emit(args, `ok: thread ${threadId} turn=${state}${reply ? `\n${reply}` : ""}`, {
      ok: true, threadId, turn: state, reply,
    });
    if (state !== "completed") ctx.exitCode = 1;
  }

  async function mutate(type, threadId, extra, already, label) {
    const c = await rpc();
    const thread = await loadThread(c, threadId);
    if (already?.(thread)) {
      emit(args, `ok: thread ${threadId} already ${label}`, { ok: true, threadId, already: true, type });
      return;
    }
    const result = await dispatch(c, { type, threadId, ...extra });
    emit(args, `ok: ${label} thread ${threadId} (sequence ${sequenceOf(result)})`, {
      ok: true, threadId, type, sequence: sequenceOf(result),
    });
  }

  switch (cmd) {
    case "login": {
      const c = { ...cfg, baseUrl: need(args, "url") };
      if (args.token && args.token !== true) c.token = args.token;
      else c.token = (await exchangePairingCode(c.baseUrl, need(args, "pair"))).access_token;
      await wsTicket(c); saveConfig(c);
      emit(args, `ok: saved ${CONFIG_PATH}`, { ok: true, path: CONFIG_PATH, baseUrl: c.baseUrl });
      return;
    }
    case "projects": {
      const shell = await shellSnapshot(await rpc());
      const rows = shell.projects.map((project) => ({
        id: project.id, title: project.title, workspaceRoot: project.workspaceRoot,
        defaultModel: project.defaultModelSelection?.model ?? null,
        defaultModelSelection: project.defaultModelSelection ?? null,
      }));
      if (args.json === true) console.log(JSON.stringify(rows, null, 2));
      else for (const project of rows) console.log(`${project.id}\t${project.title}\t${project.workspaceRoot}`);
      return;
    }
    case "models": {
      const config = await (await rpc()).request("server.getConfig", {});
      const rows = (config.providers ?? []).map((provider) => ({
        instanceId: provider.instanceId,
        driver: provider.driver,
        displayName: provider.displayName ?? provider.driver,
        enabled: provider.enabled === true,
        models: (provider.models ?? []).map((model) => ({
          slug: model.slug,
          name: model.name,
          isDefault: model.isDefault === true,
          options: (model.capabilities?.optionDescriptors ?? []).map((descriptor) => ({
            id: descriptor.id,
            type: descriptor.type,
            choices: descriptor.type === "select" ? (descriptor.options ?? []).map((choice) => choice.id) : undefined,
          })),
        })),
      }));
      if (args.json === true) console.log(JSON.stringify(rows, null, 2));
      else {
        for (const provider of rows) {
          console.log(`${provider.instanceId}\t${provider.driver}\t${provider.displayName}\tenabled=${provider.enabled ? "yes" : "no"}`);
          for (const model of provider.models) {
            console.log(`  ${model.slug}\t${model.name}${model.isDefault ? "\tdefault" : ""}`);
            for (const option of model.options) {
              const choices = option.choices ? ` ${option.choices.join("|")}` : "";
              console.log(`    ${option.id} (${option.type})${choices}`);
            }
          }
        }
      }
      return;
    }
    case "threads": {
      const c = await rpc();
      const shell = await shellSnapshot(c);
      let threads = shell.threads ?? [];
      if (args.all === true || args.archived === true) {
        const seen = new Set(threads.map((thread) => thread.id));
        for (const thread of await archivedThreads(c)) {
          if (!seen.has(thread.id)) threads.push(thread);
        }
      }
      const rows = selectThreads(threads, args)
        .map(threadRow)
        .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)));
      if (args.json === true) console.log(JSON.stringify(rows, null, 2));
      else for (const row of rows) console.log(`${row.id}\t${row.projectId}\tturn=${row.turn}\t${row.flags.join(",")}\t${row.title}`);
      return;
    }
    case "create": {
      const c = await rpc();
      const projectId = need(args, "project"), title = need(args, "title"), prompt = need(args, "prompt");
      const defaults = resolveThreadDefaults(await shellSnapshot(c), projectId);
      if (hasModelArgs(args)) {
        const reasoningId = await resolveReasoningId(c, defaults.modelSelection, args);
        defaults.modelSelection = modelSelectionFrom(defaults.modelSelection, args, reasoningId);
      }
      const runtime = opt(args, "runtime");
      const interaction = opt(args, "interaction");
      if (runtime) defaults.runtimeMode = oneOf(runtime, RUNTIME_MODES, "runtime");
      if (interaction) defaults.interactionMode = oneOf(interaction, INTERACTION_MODES, "interaction");
      const threadId = randomUUID();
      await dispatch(c, {
        type: "thread.create", threadId, projectId, title,
        modelSelection: defaults.modelSelection, runtimeMode: defaults.runtimeMode,
        interactionMode: defaults.interactionMode, branch: null, worktreePath: null, createdAt: now(),
      });
      await sendPrompt(c, threadId, prompt, defaults, null);
      return;
    }
    case "send": {
      const c = await rpc();
      const threadId = need(args, "thread");
      const thread = await loadThread(c, threadId);
      const defaults = { modelSelection: thread.modelSelection, runtimeMode: thread.runtimeMode, interactionMode: thread.interactionMode };
      const runtime = opt(args, "runtime");
      const interaction = opt(args, "interaction");
      if (runtime) defaults.runtimeMode = oneOf(runtime, RUNTIME_MODES, "runtime");
      if (interaction) defaults.interactionMode = oneOf(interaction, INTERACTION_MODES, "interaction");
      await sendPrompt(c, threadId, need(args, "prompt"), defaults, thread.latestTurn?.turnId ?? null);
      return;
    }
    case "status": case "read": {
      const threadId = need(args, "thread");
      const c = await rpc();
      try {
        const thread = await loadThread(c, threadId);
        if (args.json === true) console.log(JSON.stringify(threadView(thread), null, 2));
        else { printStatus(thread); console.log(formatTranscript(thread)); }
      } catch (error) {
        if (!isMissingThread(error)) throw error;
        const shell = (await archivedThreads(c)).find((thread) => thread.id === threadId);
        if (!shell) throw error;
        const view = {
          ...threadView({ ...shell, messages: [], activities: [] }),
          messagesUnavailable: true,
        };
        if (args.json === true) console.log(JSON.stringify(view, null, 2));
        else {
          console.log(`thread ${shell.id} "${shell.title}" turn=${shell.latestTurn ? shell.latestTurn.state : "none"} settled=${isSettled(shell) ? `yes (${shell.settledAt ?? "override"})` : "no"} archived=${shell.archivedAt ?? "no"}`);
          console.log("transcript: unavailable while archived; unarchive the thread to read messages");
        }
      }
      return;
    }
    case "settle": {
      const threadId = need(args, "thread");
      const c = await rpc();
      const thread = await loadThread(c, threadId);
      if (thread.settledOverride === "settled") {
        emit(args, `ok: thread ${threadId} already settled (${thread.settledAt})`, {
          ok: true, threadId, already: true, settledAt: thread.settledAt ?? null,
        });
        return;
      }
      const result = await dispatch(c, { type: "thread.settle", threadId });
      emit(args, `ok: settled thread ${threadId} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, sequence: sequenceOf(result),
      });
      return;
    }
    case "unsettle":
      await mutate("thread.unsettle", need(args, "thread"), { reason: "user" }, (thread) => !isSettled(thread), "unsettled");
      return;
    case "archive":
      await mutate("thread.archive", need(args, "thread"), {}, (thread) => thread.archivedAt != null, "archived");
      return;
    case "unarchive": {
      const threadId = need(args, "thread");
      const c = await rpc();
      try {
        const thread = await loadThread(c, threadId);
        if (thread.archivedAt == null) {
          emit(args, `ok: thread ${threadId} already unarchived`, { ok: true, threadId, already: true });
          return;
        }
      } catch (error) {
        if (!isMissingThread(error)) throw error;
      }
      const result = await dispatch(c, { type: "thread.unarchive", threadId });
      emit(args, `ok: unarchived thread ${threadId} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, sequence: sequenceOf(result),
      });
      return;
    }
    case "pin": {
      const orderKey = opt(args, "order-key");
      await mutate("thread.pin", need(args, "thread"), orderKey ? { orderKey } : {}, (thread) => thread.pinnedAt != null && !orderKey, "pinned");
      return;
    }
    case "unpin":
      await mutate("thread.unpin", need(args, "thread"), {}, (thread) => thread.pinnedAt == null, "unpinned");
      return;
    case "reorder-pin":
      await mutate("thread.pin.reorder", need(args, "thread"), { orderKey: need(args, "order-key") }, undefined, "reordered pin");
      return;
    case "reorder-active":
      await mutate("thread.active.reorder", need(args, "thread"), { orderKey: need(args, "order-key") }, undefined, "reordered active");
      return;
    case "snooze": {
      const until = opt(args, "until");
      const minutes = opt(args, "minutes");
      let snoozedUntil = until;
      if (!snoozedUntil && minutes !== undefined) {
        const n = Number(minutes);
        if (!Number.isFinite(n) || n <= 0) throw new Error("--minutes must be a positive number");
        snoozedUntil = new Date(Date.now() + n * 60_000).toISOString();
      }
      if (!snoozedUntil || !Number.isFinite(Date.parse(snoozedUntil))) throw new Error("pass --until <ISO-8601> or --minutes <n>");
      await mutate("thread.snooze", need(args, "thread"), { snoozedUntil }, undefined, "snoozed");
      return;
    }
    case "unsnooze":
      await mutate("thread.unsnooze", need(args, "thread"), { reason: "user" }, (thread) => !thread.snoozedUntil, "unsnoozed");
      return;
    case "interrupt": {
      const threadId = need(args, "thread");
      const turnId = opt(args, "turn");
      const result = await dispatch(await rpc(), {
        type: "thread.turn.interrupt", threadId, createdAt: now(), ...(turnId ? { turnId } : {}),
      });
      emit(args, `ok: interrupt thread ${threadId} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, sequence: sequenceOf(result),
      });
      return;
    }
    case "model": {
      if (!hasModelArgs(args)) throw new Error("pass --model, --instance, --effort, --option, or --clear-options");
      const threadId = need(args, "thread");
      const c = await rpc();
      const thread = await loadThread(c, threadId);
      const reasoningId = await resolveReasoningId(c, thread.modelSelection, args);
      const modelSelection = modelSelectionFrom(thread.modelSelection, args, reasoningId);
      const result = await dispatch(c, { type: "thread.meta.update", threadId, modelSelection });
      emit(args, `ok: model thread ${threadId} model=${modelSelection.model} instance=${modelSelection.instanceId} options=${formatOptions(modelSelection.options)} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, modelSelection, sequence: sequenceOf(result),
      });
      return;
    }
    case "runtime": {
      const threadId = need(args, "thread");
      const runtimeMode = oneOf(need(args, "mode"), RUNTIME_MODES, "mode");
      const result = await dispatch(await rpc(), { type: "thread.runtime-mode.set", threadId, runtimeMode, createdAt: now() });
      emit(args, `ok: runtime thread ${threadId} ${runtimeMode} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, runtimeMode, sequence: sequenceOf(result),
      });
      return;
    }
    case "interaction": {
      const threadId = need(args, "thread");
      const interactionMode = oneOf(need(args, "mode"), INTERACTION_MODES, "mode");
      const result = await dispatch(await rpc(), { type: "thread.interaction-mode.set", threadId, interactionMode, createdAt: now() });
      emit(args, `ok: interaction thread ${threadId} ${interactionMode} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, interactionMode, sequence: sequenceOf(result),
      });
      return;
    }
    case "approve": {
      const threadId = need(args, "thread");
      const requestId = need(args, "request");
      const decision = oneOf(need(args, "decision"), APPROVAL_DECISIONS, "decision");
      const result = await dispatch(await rpc(), { type: "thread.approval.respond", threadId, requestId, decision, createdAt: now() });
      emit(args, `ok: approve thread ${threadId} request ${requestId} ${decision} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, requestId, decision, sequence: sequenceOf(result),
      });
      return;
    }
    case "answer": {
      const threadId = need(args, "thread");
      const requestId = need(args, "request");
      const answers = {};
      if (opt(args, "answers")) Object.assign(answers, parseJson(need(args, "answers"), "answers"));
      for (const raw of args.answer ?? []) {
        const text = String(raw);
        const eq = text.indexOf("=");
        if (eq <= 0) throw new Error("--answer expects questionId=text");
        answers[text.slice(0, eq)] = text.slice(eq + 1);
      }
      if (Object.keys(answers).length === 0) throw new Error("pass --answers <json> or --answer questionId=text");
      const result = await dispatch(await rpc(), { type: "thread.user-input.respond", threadId, requestId, answers, createdAt: now() });
      emit(args, `ok: answer thread ${threadId} request ${requestId} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, requestId, answers, sequence: sequenceOf(result),
      });
      return;
    }
    case "dismiss": {
      const threadId = need(args, "thread");
      const requestId = need(args, "request");
      const result = await dispatch(await rpc(), { type: "thread.user-input.dismiss", threadId, requestId, createdAt: now() });
      emit(args, `ok: dismiss thread ${threadId} request ${requestId} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, requestId, sequence: sequenceOf(result),
      });
      return;
    }
    case "rename": {
      const threadId = need(args, "thread");
      const title = need(args, "title");
      const result = await dispatch(await rpc(), { type: "thread.meta.update", threadId, title });
      emit(args, `ok: rename thread ${threadId} (sequence ${sequenceOf(result)})`, { ok: true, threadId, title, sequence: sequenceOf(result) });
      return;
    }
    case "retitle": {
      const threadId = need(args, "thread");
      const result = await dispatch(await rpc(), { type: "thread.meta.update", threadId, regenerateTitle: true });
      emit(args, `ok: retitle thread ${threadId} (sequence ${sequenceOf(result)})`, { ok: true, threadId, sequence: sequenceOf(result) });
      return;
    }
    case "branch": {
      const threadId = need(args, "thread");
      const name = opt(args, "name");
      if (args.clear !== true && name === undefined) throw new Error("pass --name or --clear");
      const command = { type: "thread.meta.update", threadId, branch: args.clear === true ? null : name };
      const expected = opt(args, "expected");
      if (expected !== undefined) command.expectedBranch = expected;
      const result = await dispatch(await rpc(), command);
      emit(args, `ok: branch thread ${threadId} ${command.branch ?? "(cleared)"} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, branch: command.branch, sequence: sequenceOf(result),
      });
      return;
    }
    case "worktree": {
      const threadId = need(args, "thread");
      const path = opt(args, "path");
      if (args.clear !== true && path === undefined) throw new Error("pass --path or --clear");
      const worktreePath = args.clear === true ? null : path;
      const result = await dispatch(await rpc(), { type: "thread.meta.update", threadId, worktreePath });
      emit(args, `ok: worktree thread ${threadId} ${worktreePath ?? "(cleared)"} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, worktreePath, sequence: sequenceOf(result),
      });
      return;
    }
    case "autosettle": {
      const threadId = need(args, "thread");
      if (args.on === true && args.off === true) throw new Error("pass only one of --on or --off");
      if (args.on !== true && args.off !== true) throw new Error("pass --on or --off");
      const enabled = args.on === true;
      const result = await dispatch(await rpc(), { type: "thread.auto-settle.set", threadId, enabled });
      emit(args, `ok: autosettle thread ${threadId} ${enabled ? "on" : "off"} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, enabled, sequence: sequenceOf(result),
      });
      return;
    }
    case "stop": {
      const threadId = need(args, "thread");
      const command = { type: "thread.session.stop", threadId, createdAt: now() };
      if (args["only-if-settled"] === true) command.onlyIfSettled = true;
      const result = await dispatch(await rpc(), command);
      emit(args, `ok: stop thread ${threadId} (sequence ${sequenceOf(result)})`, { ok: true, threadId, sequence: sequenceOf(result) });
      return;
    }
    case "revert":
    case "revert-chat": {
      const threadId = need(args, "thread");
      const turnCount = Number(need(args, "turn-count"));
      if (!Number.isInteger(turnCount) || turnCount < 0) throw new Error("--turn-count must be an integer >= 0");
      const type = cmd === "revert" ? "thread.checkpoint.revert" : "thread.conversation.revert";
      const result = await dispatch(await rpc(), { type, threadId, turnCount, createdAt: now() });
      emit(args, `ok: ${cmd} thread ${threadId} turn-count=${turnCount} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, turnCount, sequence: sequenceOf(result),
      });
      return;
    }
    case "link-pr": {
      const threadId = need(args, "thread");
      const number = Number(need(args, "number"));
      if (!Number.isInteger(number) || number < 1) throw new Error("--number must be a positive integer");
      const source = oneOf(opt(args, "source") ?? "manual", PR_SOURCES, "source");
      const command = {
        type: "thread.pull-request.link", threadId,
        host: need(args, "host"), repository: need(args, "repository"), number,
        url: need(args, "url"), source,
      };
      const result = await dispatch(await rpc(), command);
      emit(args, `ok: link-pr thread ${threadId} ${command.repository}#${number} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, ...command, sequence: sequenceOf(result),
      });
      return;
    }
    case "unlink-pr": {
      const threadId = need(args, "thread");
      const number = Number(need(args, "number"));
      if (!Number.isInteger(number) || number < 1) throw new Error("--number must be a positive integer");
      const command = {
        type: "thread.pull-request.unlink", threadId,
        host: need(args, "host"), repository: need(args, "repository"), number,
      };
      const result = await dispatch(await rpc(), command);
      emit(args, `ok: unlink-pr thread ${threadId} ${command.repository}#${number} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, ...command, sequence: sequenceOf(result),
      });
      return;
    }
    case "forget":
    case "delete": {
      const threadId = need(args, "thread");
      const result = await dispatch(await rpc(), { type: "thread.delete", threadId });
      emit(args, `ok: forgot thread ${threadId} (sequence ${sequenceOf(result)})`, {
        ok: true, threadId, sequence: sequenceOf(result),
      });
      return;
    }
    case "project-create": {
      const projectId = opt(args, "id") ?? randomUUID();
      const command = {
        type: "project.create", projectId, title: need(args, "title"),
        workspaceRoot: need(args, "workspace"), createdAt: now(),
      };
      if (args["create-root"] === true) command.createWorkspaceRootIfMissing = true;
      const result = await dispatch(await rpc(), command);
      emit(args, `ok: project-create ${projectId} (sequence ${sequenceOf(result)})`, {
        ok: true, projectId, sequence: sequenceOf(result),
      });
      return;
    }
    case "project-update": {
      const projectId = need(args, "project");
      const command = { type: "project.meta.update", projectId };
      const title = opt(args, "title");
      const workspace = opt(args, "workspace");
      const favicon = opt(args, "favicon");
      if (title) command.title = title;
      if (workspace) command.workspaceRoot = workspace;
      if (opt(args, "auto-pull") !== undefined || args["auto-pull"] === true) command.autoPull = parseBool(args["auto-pull"], "auto-pull");
      if (opt(args, "env")) command.defaultThreadEnvMode = oneOf(need(args, "env"), ENV_MODES, "env");
      if (args["clear-env"] === true) command.defaultThreadEnvMode = null;
      if (favicon) command.faviconPath = favicon;
      if (args["clear-favicon"] === true) command.faviconPath = null;
      if (opt(args, "icon-json")) command.projectIcon = parseJson(need(args, "icon-json"), "icon-json");
      if (args["clear-icon"] === true) command.projectIcon = null;
      if (opt(args, "scripts-json")) command.scripts = parseJson(need(args, "scripts-json"), "scripts-json");
      if (opt(args, "default-model-json")) command.defaultModelSelection = parseJson(need(args, "default-model-json"), "default-model-json");
      if (args["clear-default-model"] === true) command.defaultModelSelection = null;
      if (Object.keys(command).length === 2) throw new Error("pass at least one field to change");
      const result = await dispatch(await rpc(), command);
      emit(args, `ok: project-update ${projectId} (sequence ${sequenceOf(result)})`, {
        ok: true, projectId, sequence: sequenceOf(result),
      });
      return;
    }
    case "project-delete": {
      const projectId = need(args, "project");
      const command = { type: "project.delete", projectId };
      if (args.force === true) command.force = true;
      const result = await dispatch(await rpc(), command);
      emit(args, `ok: project-delete ${projectId} (sequence ${sequenceOf(result)})`, {
        ok: true, projectId, sequence: sequenceOf(result),
      });
      return;
    }
    case "search": {
      const query = need(args, "query");
      if (query.length < 2 || query.length > 200) throw new Error("--query must be 2 to 200 characters");
      const payload = { query };
      const limit = opt(args, "limit");
      if (limit !== undefined) {
        const n = Number(limit);
        if (!Number.isInteger(n) || n < 1 || n > 50) throw new Error("--limit must be an integer from 1 to 50");
        payload.limit = n;
      }
      const result = await (await rpc()).request("orchestration.searchThreads", payload);
      const matches = result?.matches ?? [];
      if (args.json === true) console.log(JSON.stringify(result, null, 2));
      else if (matches.length === 0) console.log("no matches");
      else for (const match of matches) console.log(`${match.threadId}\t${match.projectId}\t${match.source}\t${match.snippet}`);
      return;
    }
    case "diff": {
      const threadId = need(args, "thread");
      const toTurnCount = Number(need(args, "to"));
      if (!Number.isInteger(toTurnCount) || toTurnCount < 0) throw new Error("--to must be an integer >= 0");
      const payload = { threadId, toTurnCount };
      if (args["ignore-whitespace"] === true) payload.ignoreWhitespace = true;
      const from = opt(args, "from");
      let tag = "orchestration.getFullThreadDiff";
      if (from !== undefined) {
        const fromTurnCount = Number(from);
        if (!Number.isInteger(fromTurnCount) || fromTurnCount < 0) throw new Error("--from must be an integer >= 0");
        if (fromTurnCount > toTurnCount) throw new Error("--from must be <= --to");
        payload.fromTurnCount = fromTurnCount;
        tag = "orchestration.getTurnDiff";
      }
      const result = await (await rpc()).request(tag, payload);
      const text = `ok: diff thread ${threadId} ${from ?? "full"}..${toTurnCount}\n${result?.diff ?? ""}`;
      emit(args, text, { ok: true, ...result });
      return;
    }
    case "workflow-script": {
      const threadId = need(args, "thread");
      const scriptPath = need(args, "path");
      const result = await (await rpc()).request("orchestration.getWorkflowScript", { threadId, scriptPath });
      emit(args, result?.contents ?? "", { ok: true, ...result });
      return;
    }
    case "batch": {
      for (const line of readFileSync(need(args, "file"), "utf8").split("\n").filter((l) => l.trim())) {
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
const topic = args._[0] === "help" ? args._[1] : args.help ? args._[0] : (!args._[0] ? "" : null);
if (topic !== null) {
  process.exit(printHelp(topic || undefined) ? 0 : 2);
}
const ctx = { exitCode: 0 };
const hardTimeout = setTimeout(() => {
  console.error("error: overall timeout"); process.exit(1);
}, (Number(args.timeout ?? 900) + 60) * 1000);
hardTimeout.unref();
try {
  await run(args, loadConfig(), ctx);
} catch (e) {
  if (args.json === true) console.log(JSON.stringify({ ok: false, error: e.message }));
  else console.error(`error: ${e.message}`);
  ctx.exitCode = 1;
} finally {
  ctx.rpc?.close();
}
process.exit(ctx.exitCode);
