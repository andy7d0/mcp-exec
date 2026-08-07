# mcp-exec

A minimal **stdio MCP server** that exposes a shell command executor to LLM agents.
It is deliberately **not** a terminal emulator and **not** a collection of specialized
tools: it runs `$SHELL -c <command>` with ordinary pipes (no PTY), tracks every
invocation as a *job*, and gives the agent a small, complete control plane over those
jobs — start, wait, poll, read output, write stdin, signal, list.

- **Transport:** JSON-RPC 2.0 over stdio, newline-delimited (one JSON value per line), per the MCP stdio convention.
- **Protocol version:** `2024-11-05`.
- **Platforms:** Unix (Linux/macOS). No Windows.
- **Version:** 1.0.0

---

## Why it is shaped this way

| Problem with naive shell tools | What mcp-exec does |
|---|---|
| Blocking calls time out on long commands | Jobs are async: `execute(nowait=true)` returns a `job_id`; `wait`/`status`/`output` poll it |
| Huge output blows the context window | Each stream keeps a **sliding window of the last N bytes** (default 1 MiB); clients read by `tail` lines or byte ranges, and are told exactly how much was dropped |
| "Is it done yet?" guesswork | `wait` blocks until any listed job finishes or a timeout; `timeout<=0` is an instant poll |
| Zombies/orphans from backgrounded children | Jobs run in their own **process group**; `kill` signals the whole group and waits for it to drain; as PID 1 the server reaps orphans itself |
| Signals that get ignored | `kill` verifies **actual death** (not delivery); `"ASSURED"` escalates SIGTERM → SIGKILL |
| Stale job state piling up | Terminal results are **delivered exactly once**, then the job is deleted |
| Hung requests | `notifications/cancelled` aborts blocking calls (`-32800`); long calls emit `notifications/progress` |

**The three invariants clients can rely on:**

1. **Exactly-once results.** A terminal result is delivered once (by `execute`, `wait`, or `kill`), and the job is deleted immediately afterwards. After that, every tool answers `unknown job_id`.
2. **Offsets are bytes.** All `from`/`offset` values are absolute byte positions from the start of the stream. Pagination advances with `from = offset + bytes` — both fields are always present in output slices.
3. **`list_jobs` shows everything waitable.** Running jobs *and* finished jobs whose result hasn't been collected yet. If it's listed, `wait` can deliver it.

---

## Quick start

Files: `main.go` (the server), `Containerfile`, `test_mcp_exec.py` (the e2e suite).

```bash
# build the image (Go multi-stage build; runtime = alpine + tini)
podman build -t mcp-exec:1.0.0 .

# sanity check: talk to it by hand
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | podman run -i --rm mcp-exec:1.0.0
```

### Client configuration

Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "exec": {
      "command": "podman",
      "args": ["run", "-i", "--rm", "mcp-exec:1.0.0"]
    }
  }
}
```

OpenCode (`opencode.json`):

```json
{
  "mcp": {
    "exec": {
      "type": "local",
      "command": ["podman", "run", "-i", "--rm", "mcp-exec:1.0.0"]
    }
  }
}
```

Any MCP client that launches a stdio subprocess works the same way.

---

## Tools

### `execute` — run a command as a job

| Arg | Type | Meaning |
|---|---|---|
| `command` | string, required | Shell command line (`$SHELL -c <command>`) |
| `cwd` | string | Working directory (default: server's cwd) |
| `env` | object | Vars layered **on top of** the server environment (merged, not replaced) |
| `stdin` | string | Data written to the process's stdin immediately |
| `close_stdin` | bool | Close stdin (EOF) after writing. Default `false` (stays open for `input`) |
| `nowait` | bool | Return `{job_id, state:"running"}` immediately instead of blocking |
| `capture` | object | `{on_success, on_error}` capture policy — see [Stream capture](#stream-capture-specs) |

- Without `nowait`: blocks until the job finishes (like `wait([job])` without timeout) and returns the terminal result. **That delivery reaps the job.**
- Start failures (bad `cwd`, etc.) are returned immediately as a fail result and reaped, with or without `nowait`.
- Default capture policy: `on_success → stdout`, `on_error → stderr`.
- Large `stdin` to a child that never reads will block until the child drains or dies (cancellable).

### `wait` — block until any listed job settles

| Arg | Type | Meaning |
|---|---|---|
| `job_ids` | string[], required | Jobs to watch |
| `timeout` | number | Seconds. Default: `MCP_EXEC_DEFAULT_TIMEOUT`. **`<= 0` = instant poll** |

Returns an array (same order as `job_ids`): finished jobs carry their captured output per policy; running jobs carry state only. **Jobs whose terminal result appears in the response are deleted.** Duplicate ids are fine.

### `status` — state only, never deletes

`{job_id}` → `{job_id, state, exit_code?, signal?, start_error?}`. Safe to spam.

### `output` — read captured buffers, live or after exit

| Arg | Type | Meaning |
|---|---|---|
| `job_id` | string, required | |
| `stdout` / `stderr` | stream-capture | What slice of each stream to return; omit both ⇒ equivalent to `status` |

Never deletes the job. Works on running jobs (buffers are live).

### `input` — write to a running job's stdin

`{job_id, data, close?}` — `close=true` sends EOF after the data. Errors on terminal jobs and on double-close.

### `kill` — signal a job's process group, verified

| Arg | Type | Meaning |
|---|---|---|
| `job_id` | string, required | |
| `signal` | string | Name (`"TERM"`, `"SIGKILL"`, …), number (`"9"`), or `"ASSURED"`. Default `TERM` |
| `timeout` | number | Seconds to wait for the tree to actually die after each signal. Default 5; `0` = don't wait |

- Signals go to the **entire process group** (shell + everything it spawned).
- A signal is only a request; `kill` waits for observed death (child reaped **and** group drained). Survivors get `terminated:false` plus a `detail` hint — the job stays accessible.
- `"ASSURED"` = SIGTERM, then SIGKILL if the tree survives the grace window (`escalated:true` in the reply).
- If the tree dies: the terminal result is returned and the job is deleted. If it was already fully gone: `already_terminated:true` with the stored result. If only the shell died but children survive, the group is still signalled.

### `list_jobs` — everything still waitable

No args. `{"jobs": [{"job_id", "state"}, …]}`, oldest first. Includes running jobs and terminal-but-uncollected jobs; reaped jobs never appear.

---

## Stream capture specs

A stream argument accepts:

- `"none"` — omit (default), `"all"` — the whole retained window
- `{"tail": N}` — last N lines (endings preserved)
- `{"from": P, "length": N?, "unaligned": bool?}` — byte range

Semantics:

- **`from` is an absolute byte offset from the start of the stream**, regardless of what the window currently retains.
- **Character alignment (default):** if `from` points inside a multi-byte UTF-8 character the read extends back to that character's first byte; if `from+length` points inside a character the read extends forward past its last byte. No character is ever split. `"unaligned": true` restores exact byte slicing.
- Every slice reports `offset` (absolute position of the first returned byte, post-alignment) and `bytes` (bytes actually returned). **Advance pagination with `from = offset + bytes`.**
- **Head truncation:** each stream keeps only the last `maxOutputBytes`. When a read reaches into the dropped head, the reply sets `truncated:true`, reports the missing count in `dropped_bytes`, and serves from the oldest retained byte. `total_bytes` always reflects everything ever written.
- Data is UTF-8 sanitized (invalid bytes → U+FFFD), so responses are always valid JSON; `total_bytes`/`offset`/`bytes` still count raw bytes.

---

## Job lifecycle

```
execute ──▶ running ──▶ success | error ──▶ result delivered once ──▶ deleted
   │            │              ▲                      ▲
   └ nowait ────┴── status/output/input live ── wait / kill deliver
```

- **Delivery deletes.** Blocking `execute`, `wait` responses, and successful `kill`s deliver the terminal result exactly once and remove the job from the registry. Afterwards every tool answers `unknown job_id`.
- `status` and `output` never delete; finished-but-uncollected jobs stay visible in `list_jobs` until somebody collects them.
- **Cancellation:** clients send `notifications/cancelled` with the request id. Blocking `execute`/`wait`/`kill` abort with JSON-RPC error `-32800`. A cancelled *blocking* `execute` tears down its job (TERM→KILL, then reap), because the caller never received the `job_id`.
- **Progress:** include `_meta.progressToken` in a `tools/call`; long blocking operations emit `notifications/progress` (`{progressToken, progress, message}`) every `MCP_EXEC_PROGRESS_INTERVAL` seconds.
- **Graceful shutdown:** when stdin reaches EOF the server cancels all in-flight requests and exits promptly (bounded wait on teardowns).

## Process management

- Every job runs as its own **process group** (`Setpgid`); `kill` signals and drains the whole group, so backgrounded children don't survive as port-holding orphans.
- Output pipes are owned by the server: a job becomes terminal when the direct child exits, even if descendants still hold the pipes — and their output keeps being captured until they close them.
- When running as **PID 1** (container without an init), the server reaps reparented orphans itself (with correct routing of job-child exit statuses); otherwise `tini` in the image does it. Both paths are covered by tests.

---

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `MCP_EXEC_DEFAULT_TIMEOUT` | `60` | Default `wait` timeout, seconds (float) |
| `MCP_EXEC_MAX_OUTPUT_BYTES` | `1048576` | Retained window per stream per job |
| `MCP_EXEC_PROGRESS_INTERVAL` | `10` | Seconds between progress notifications |

---

## Building

```bash
# plain binary (Linux/macOS)
CGO_ENABLED=0 go build -o mcp-exec .

# container image (recommended)
podman build -t mcp-exec:1.0.0 .
```

The Containerfile is a multi-stage build: `golang:*-alpine` → static binary → `alpine` runtime with `tini` (optional belt-and-suspenders PID 1) and `bash`/`procps`/`coreutils` for predictable command behavior.

## Tests

`test_mcp_exec.py` is a self-contained pytest suite (≈109 tests) that builds the image and drives real containers over `podman run -i` via newline-delimited JSON-RPC.

```bash
pytest -v test_mcp_exec.py        # ~3 min including the image build; needs podman
```

Coverage: protocol basics; execute/wait/status/output/input semantics; capture policies; the byte-window with exact suffix/truncation math (KB and MB scale); character alignment and byte-offset pagination; binary torture (NULs, invalid UTF-8, base64 fidelity); stdin incl. 200 KB writes and cancellation; kill (TERM/KILL/ASSURED, survivors, orphaned grandchildren, concurrent kill races); list_jobs; cancellation incl. late/double/mismatched cancels; progress notifications; bare-PID-1 reaping; graceful shutdown; concurrent-request behavior.

Note: run without `pytest-xdist` — the suite uses session-scoped servers with shared job registries.

---

## Limitations (known & accepted)

- Processes that call `setsid()` detach from the process group and are out of `kill`'s reach.
- Processes stuck in uninterruptible sleep (D state) survive even SIGKILL until they unblock; `kill` reports this in `detail`.
- With a continuously streaming orphan, terminal delivery can be delayed up to ~500 ms (settle window).
- Job registry is in-memory: a server restart loses bookkeeping (a stdio server's lifetime is its client session).
- Sequential, guessable job ids — fine for single-client stdio; revisit before any network exposure.
- Theoretical pgid-reuse race in the signal-0 group probe.

## Security

- The server executes arbitrary commands with the caller's privileges — that is the product. There is no sandboxing, and `env` overrides can set anything the shell honors.
- The stdio transport has no authentication; the security boundary is whatever launches the process. Run it rootless (`podman`), don't mount sensitive volumes into the container, and never expose it over HTTP without an authenticating proxy in front.
