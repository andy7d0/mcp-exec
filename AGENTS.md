# AGENTS.md

Guide for AI coding agents working in this repository. `README.md` is the human/operator doc;
this file tells you how to build, test, and — above all — how **not** to break this codebase.

## Project

mcp-exec is a minimal stdio MCP server (Go, single file) exposing a shell command executor as an
async job manager. Tools: `execute`, `wait`, `status`, `output`, `input`, `kill`, `list_jobs`.
JSON-RPC 2.0 over stdio, newline-delimited. Unix-only (Linux/macOS) by design.

## Repo layout

- `main.go` — the entire server. `go.mod` (module `mcp-executor`, Go 1.24, stdlib only); builds with `go build .`.
- `Containerfile` — multi-stage build: `golang:*-alpine` → `alpine` + `tini` + bash/procps/coreutils.
- `test_mcp_exec.py` — self-contained pytest e2e suite (~109 tests) driving real containers via `podman run -i`.
- `README.md` — usage, tool reference, semantics, deployment.

## Commands

```bash
podman build -t mcp-exec:1.0.0 .          # build the image
pytest -v test_mcp_exec.py                # full suite (~3 min incl. build; needs podman + pytest)
pytest -v test_mcp_exec.py::TestKill      # one class
pytest -v test_mcp_exec.py::TestKill::test_kill_term   # one test

# raw stdio smoke test
echo '{"jsonrpc":"2.0","id":1,"method":"ping"}' | podman run -i --rm mcp-exec:1.0.0
```

- Do **not** use pytest-xdist (`-n`): the suite uses session-scoped server containers with shared job registries.
- `podman rmi mcp-exec-test` forces a rebuild after Go changes (the image fixture caches by tag).

## Invariants — do not break

1. **Terminal results are delivered exactly once, then the job is reaped.** Every delivery path
   (blocking `execute`, start-failure, `wait` results, successful `kill`) calls `m.remove(job_id)`.
   `status`/`output` never reap.
2. **`from`/`offset`/`bytes` are BYTES, absolute from stream start** — even when the head was
   truncated. Reads reaching into the truncated head set `truncated:true`,
   `dropped_bytes = dropped - from`, and serve from the oldest retained byte.
3. **`from` reads are character-aligned by default** (extend outward so no UTF-8 char is split);
   `"unaligned": true` restores exact byte slicing. `offset` and `bytes` are **always emitted**
   (no `omitempty` on `captureResult.Offset`/`Bytes`).
4. **kill verifies actual death** (child reaped AND process group drained), never signal delivery.
   Signals go to `-pgid` (the whole group). `"ASSURED"` = TERM → wait → KILL.
5. **Captured data is UTF-8 sanitized** (`toSafeText`) before JSON embedding; byte counts
   (`total_bytes`, `offset`, `bytes`) always count raw bytes.
6. **Blocking tools (`execute`/`wait`/`kill`) honor ctx** and map `context.Canceled` to JSON-RPC
   `-32800`. `wait`'s per-job waiter goroutines select on `ctx.Done` — leaking them is a bug.
7. **capBuffer keeps the LAST `max` bytes** (sliding tail window); `dropped = total − retained`.
   The writer never blocks or errors on the child.
8. **Responses are one-line JSON values**; all stdout writes are serialized under `outMu`.

## Pitfalls — lessons paid for in debugging

- Never pass `capBuffer` (or any non-`*os.File`) as `cmd.Stdout`/`cmd.Stderr`: os/exec copies via
  internal goroutines and `cmd.Wait()` blocks until their EOF — which never arrives while a
  backgrounded descendant holds the pipe ("daemon &" hangs forever). Capture via our own
  `os.Pipe`; close the parent write ends after `Start()`.
- A job must turn terminal when the **direct child** exits, even if descendants still stream;
  `settleCapture` (≤500 ms) drains in-flight bytes before the terminal state becomes observable.
  Reader goroutines intentionally outlive `cmd.Wait()` and even job reaping — don't "fix" that.
- `strings.ToValidUTF8` replaces a *run* of consecutive invalid bytes with **one** U+FFFD, not one
  per byte. Tests needing 1:1 byte↔char math must isolate invalid bytes between valid ones.
- `cmd.Wait()` returning `syscall.ECHILD` means the PID-1 orphan reaper won the waitpid race; the
  status arrives via `stolenCh`/`stolenWS`. Preserve that routing.
- stdin writes beyond ~64 KB block on children that never read; always go through
  `writeStdinCancellable`, never inline `writeStdin` in request paths.
- `omitempty` on semantically-load-bearing numeric fields silently breaks pagination clients.
- In test shell commands, `pgrep`/`pkill -f` matches the wrapper shell's own cmdline — use the
  bracket trick (`'sleep 3[0]'`).
- Tests that spawn jobs must clean up (`cleanup_job` in `finally`): session servers are shared,
  and a leaked `sleep 30` breaks later pgrep-based assertions. Keep timing windows wide.

## Style

- gofmt. Single file with `// -----` section banners — keep that structure.
- Comments explain **why** (os/exec and signal semantics especially), not what.
- Tool-level failures are returned as values and become `isError` tool results; protocol failures
  are JSON-RPC errors; cancellation is `errors.Is(err, context.Canceled)` → `-32800`.
- Stdlib only. No new dependencies; the `go.mod` stays bare (module name + `go` directive only).
- Unix-only is intentional (`syscall.Setpgid`, `WaitStatus`, `Kill`); don't add Windows ifdefs.

## Adding a tool

1. `xxxArgs` struct.
2. `(m *manager) toolXxx(ctx, raw, [rep])` — parse, `m.get(job)`, act. Reap only when delivering
   a terminal result.
3. Case in `handleToolsCall`; pass `rep` only if the tool can block.
4. `toolDef` entry with full `inputSchema`.
5. Tests: happy path + error paths + reaping semantics + (if blocking) cancellation behavior.

## Adding server behavior

- Anything blocking takes `ctx`, emits progress via `*progressReporter`, and unblocks on cancel.
- Any goroutine waiting on `j.done` also selects on `ctx.Done` (request lifetime) unless it is
  deliberately request-outliving (capture readers, `teardownWG`) — and says so in a comment.
- New env knob → `loadConfig` + header comment + README table; check test fixtures, which pin
  specific values via container env.

## Env knobs

`MCP_EXEC_DEFAULT_TIMEOUT` (60 s), `MCP_EXEC_MAX_OUTPUT_BYTES` (1 MiB),
`MCP_EXEC_PROGRESS_INTERVAL` (10 s).

## Known limitations — accepted; discuss before "fixing"

`setsid()` daemons escape the group · D-state processes survive SIGKILL until they unblock ·
Unix-only · sequential guessable job ids · in-memory registry (restart loses bookkeeping) ·
≤500 ms settle delay with a streaming orphan · theoretical pgid-reuse race in the signal-0 probe ·
no logging yet · no job-count cap yet.
