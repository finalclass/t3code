# t3-alpha

Zero-dependency Node (>= 22) console client for a T3 Code server (wire format of v0.0.45).

Each command gets a websocket ticket from `POST /api/auth/websocket-ticket`, sends one Effect RPC request, waits for the `Exit` frame, and closes the socket. Turn waits (`create` and `send` without `--no-wait`) poll the thread until the new turn leaves `running`, then close. `--json` prints one JSON document instead of text. A non-zero exit means the command failed or the turn did not complete.

The CLI does not choose a model. `create` copies the project's server-side default, or the latest thread in that project when the project has none, because `thread.create` requires a selection. `model`, and `--model` / `--effort` / `--option` on `create` or `send`, are the only ways to send a `modelSelection`.

Config: `~/.config/t3-alpha.json` (`baseUrl`, `token`), or `T3_ALPHA_URL` / `T3_ALPHA_TOKEN`.

## Several hosts

`~/.config/t3-alpha-hosts.json` (or `T3_ALPHA_HOSTS`) names the servers. Each entry points at an existing config file, so tokens stay where they are; inline `baseUrl` / `token` also work.

```json
{"hosts":[
  {"name":"rog","config":"~/.config/t3-alpha.json"},
  {"name":"archea","config":"~/.config/t3-alpha-archea.json"}
]}
```

```
node scripts/t3-alpha/t3-alpha.mjs hosts                       # name, baseUrl, online/offline
node scripts/t3-alpha/t3-alpha.mjs threads --all --unsettled   # every host in parallel, host column first
node scripts/t3-alpha/t3-alpha.mjs --host archea threads --unsettled
node scripts/t3-alpha/t3-alpha.mjs read --thread archea:<id>
```

`threads --all` gives each host its own socket and a 5 s deadline (`--host-timeout <sec>`). A host that does not answer prints `<host>\toffline: <reason>` on stderr, and the command still exits 0 when at least one host answered. `--json` prints `{"hosts":[...],"threads":[{"host":...}]}`. `--host` / `--machine` work on every command; `link-pr` and `unlink-pr` keep `--host` for the git host, so use `--machine` or `host:threadId` there. `T3_ALPHA_URL` / `T3_ALPHA_TOKEN` apply only to the default config.

`threads --all` used to mean "include archived"; that is now `--include-archived`.

```
node scripts/t3-alpha/t3-alpha.mjs login --url http://127.0.0.1:5773 --pair <code from `t3 pair`>
node scripts/t3-alpha/t3-alpha.mjs projects
node scripts/t3-alpha/t3-alpha.mjs models
node scripts/t3-alpha/t3-alpha.mjs threads --project <id> --unsettled
node scripts/t3-alpha/t3-alpha.mjs threads --archived
node scripts/t3-alpha/t3-alpha.mjs threads --all --unsettled
node scripts/t3-alpha/t3-alpha.mjs search --query "zgłoszenie"
node scripts/t3-alpha/t3-alpha.mjs create --project <id> --title "Zgłoszenie 588" --prompt "..."
node scripts/t3-alpha/t3-alpha.mjs send --thread <id> --prompt "..." --no-wait
node scripts/t3-alpha/t3-alpha.mjs read --thread <id>
node scripts/t3-alpha/t3-alpha.mjs settle --thread <id>
node scripts/t3-alpha/t3-alpha.mjs unsettle --thread <id>
node scripts/t3-alpha/t3-alpha.mjs archive --thread <id>
node scripts/t3-alpha/t3-alpha.mjs unarchive --thread <id>
node scripts/t3-alpha/t3-alpha.mjs pin --thread <id>
node scripts/t3-alpha/t3-alpha.mjs unpin --thread <id>
node scripts/t3-alpha/t3-alpha.mjs snooze --thread <id> --minutes 30
node scripts/t3-alpha/t3-alpha.mjs unsnooze --thread <id>
node scripts/t3-alpha/t3-alpha.mjs interrupt --thread <id>
node scripts/t3-alpha/t3-alpha.mjs model --thread <id> --effort low
node scripts/t3-alpha/t3-alpha.mjs runtime --thread <id> --mode approval-required
node scripts/t3-alpha/t3-alpha.mjs interaction --thread <id> --mode plan
node scripts/t3-alpha/t3-alpha.mjs approve --thread <id> --request <id> --decision accept
node scripts/t3-alpha/t3-alpha.mjs answer --thread <id> --request <id> --answer q1=yes
node scripts/t3-alpha/t3-alpha.mjs dismiss --thread <id> --request <id>
node scripts/t3-alpha/t3-alpha.mjs forget --thread <id>
```

`t3-alpha help <command>` prints that command's flags. `read` is the same command as `status`. `delete` is the same command as `forget`.

Also: `rename`, `retitle`, `branch`, `worktree`, `autosettle`, `stop`, `revert`, `revert-chat`, `reorder-pin`, `reorder-active`, `link-pr`, `unlink-pr`, `diff`, `workflow-script`, `project-create`, `project-update`, `project-delete`, and `batch`.

`threads --archived` and `read` of an archived thread use `orchestration.getArchivedShellSnapshot`. The live shell and `subscribeThread` omit archived threads, so the transcript comes back after `unarchive`. Snooze and settle are rejected by the server while a user message from the last two minutes has no turn yet.

Mark unread, the composer stash, and queued-message steering are local app state. The v0.0.45 server has no command for them.
