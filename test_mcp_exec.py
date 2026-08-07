"""
End-to-end tests for the mcp-exec MCP server, run inside a podman container.

    main.go           the mcp-exec server source
    Containerfile     builds it and runs it under tini
    test_mcp_exec.py  this file

Run:
    pytest -v test_mcp_exec.py

Requires: podman, pytest. Do NOT use pytest-xdist (-n): the suite uses
session-scoped server containers with shared job registries.
"""

import base64
import itertools
import json
import os
import queue
import subprocess
import threading
import time

import pytest

PODMAN = os.environ.get("PODMAN", "podman")
IMAGE = os.environ.get("MCP_EXEC_IMAGE", "mcp-exec-test")
HERE = os.path.dirname(os.path.abspath(__file__))

# A job that catches/ignores SIGTERM and keeps running.
SURVIVOR = "trap '' TERM; while :; do sleep 0.2; done"
# Well above the 64KB kernel pipe buffer.
BIG_STDIN = "x" * 200_000


# ---------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------

class ToolError(Exception):
    """The server answered a tools/call with isError=true."""


class MCPClient:
    """Speaks newline-delimited JSON-RPC 2.0 with `podman run -i <image>`."""

    def __init__(self, image, env=None, command=None):
        args = [PODMAN, "run", "-i", "--rm"]
        for k, v in sorted((env or {}).items()):
            args += ["-e", f"{k}={v}"]
        args.append(image)
        if command:
            args += command
        self.proc = subprocess.Popen(
            args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        self._next_id = itertools.count(1)
        self._pending = {}
        self._lock = threading.Lock()
        self.notifications = []
        self.stderr_tail = []
        self.initialize_response = None
        threading.Thread(target=self._read_stdout, daemon=True).start()
        threading.Thread(target=self._read_stderr, daemon=True).start()

    # -- plumbing -------------------------------------------------------

    def _read_stdout(self):
        for raw in self.proc.stdout:
            line = raw.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            rid = msg.get("id")
            q = None
            with self._lock:
                if rid is not None:
                    q = self._pending.get(rid)
                if q is None and "method" in msg:
                    self.notifications.append(msg)
            if q is not None:
                q.put(msg)
        with self._lock:
            queues = list(self._pending.values())
        for q in queues:
            q.put(None)

    def _read_stderr(self):
        for raw in self.proc.stderr:
            self.stderr_tail.append(raw)
            if len(self.stderr_tail) > 200:
                self.stderr_tail.pop(0)

    def _stderr(self):
        return b"".join(self.stderr_tail).decode(errors="replace")[-2000:]

    def _send(self, obj):
        assert self.proc.poll() is None, f"server process died; stderr: {self._stderr()}"
        self.proc.stdin.write((json.dumps(obj) + "\n").encode())
        self.proc.stdin.flush()

    def send_raw(self, text):
        self.proc.stdin.write(text.encode() if isinstance(text, str) else text)
        self.proc.stdin.flush()

    def drain_notifications(self):
        with self._lock:
            out, self.notifications = self.notifications, []
        return out

    # -- JSON-RPC ---------------------------------------------------------

    def notify(self, method, params=None):
        obj = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            obj["params"] = params
        self._send(obj)

    def request_async(self, method, params=None):
        rid = next(self._next_id)
        q = queue.Queue()
        with self._lock:
            self._pending[rid] = q
        obj = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            obj["params"] = params
        self._send(obj)
        return rid, q

    def request(self, method, params=None, timeout=30):
        rid, q = self.request_async(method, params)
        try:
            msg = q.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(f"no response to {method} (id={rid}) within {timeout}s")
        finally:
            with self._lock:
                self._pending.pop(rid, None)
        if msg is None:
            raise AssertionError(f"server closed the connection; stderr: {self._stderr()}")
        return msg

    def tool(self, name, arguments=None, timeout=30):
        msg = self.request("tools/call", {"name": name, "arguments": arguments or {}}, timeout=timeout)
        assert "result" in msg, f"unexpected JSON-RPC error: {msg}"
        text = msg["result"]["content"][0]["text"]
        if msg["result"].get("isError"):
            raise ToolError(text)
        return json.loads(text)

    def tool_error(self, name, arguments=None, timeout=30):
        with pytest.raises(ToolError) as ei:
            self.tool(name, arguments, timeout=timeout)
        return str(ei.value)

    # -- lifecycle ---------------------------------------------------------

    def handshake(self):
        self.initialize_response = self.request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "pytest-mcp-exec", "version": "0"},
            },
        )
        self.notify("notifications/initialized")

    def close(self):
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=10)


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------

@pytest.fixture(scope="session")
def image():
    p = subprocess.run(
        [PODMAN, "build", "-t", IMAGE, "-f", "Containerfile", HERE],
        cwd=HERE, capture_output=True, text=True,
    )
    if p.returncode != 0:
        pytest.fail(f"podman build failed:\n{p.stdout}\n{p.stderr}")
    return IMAGE


@pytest.fixture(scope="session")
def server(image):
    # MCP_EXEC_DEFAULT_TIMEOUT=3: quick default-timeout test.
    # MCP_EXEC_PROGRESS_INTERVAL=1: quick progress-notification tests.
    c = MCPClient(image, env={
        "MCP_EXEC_DEFAULT_TIMEOUT": "3",
        "MCP_EXEC_PROGRESS_INTERVAL": "1",
    })
    c.handshake()
    yield c
    c.close()


@pytest.fixture(scope="session")
def smallbuf(image):
    c = MCPClient(image, env={"MCP_EXEC_MAX_OUTPUT_BYTES": "2048"})
    c.handshake()
    yield c
    c.close()


@pytest.fixture(scope="session")
def mbuf(image):
    c = MCPClient(image, env={"MCP_EXEC_MAX_OUTPUT_BYTES": "1048576"})
    c.handshake()
    yield c
    c.close()


@pytest.fixture()
def bare_server(image):
    # No tini: the server runs as PID 1 and its orphan reaper must work.
    c = MCPClient(image, command=["/usr/local/bin/mcp-exec"])
    c.handshake()
    yield c
    c.close()


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def wait_until(fn, timeout=10, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        ok, value = fn()
        if ok:
            return value
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout:.0f}s")


def run(client, command, **kw):
    args = {"command": command}
    args.update(kw)
    return client.tool("execute", args)


def spawn(client, command, **kw):
    res = run(client, command, nowait=True, **kw)
    assert res["state"] == "running"
    return res["job_id"]


def poll_terminal(client, job_id, timeout=10):
    def check():
        st = client.tool("status", {"job_id": job_id})["state"]
        return (st != "running", st)
    return wait_until(check, timeout=timeout)


def reap(client, job_id):
    results = client.tool("wait", {"job_ids": [job_id], "timeout": 10})
    assert len(results) == 1
    with pytest.raises(ToolError, match="unknown job_id"):
        client.tool("status", {"job_id": job_id})
    return results[0]


def cleanup_job(client, job_id):
    try:
        client.tool("kill", {"job_id": job_id, "signal": "KILL", "timeout": 8})
    except ToolError:
        pass


def listed(client):
    return client.tool("list_jobs")["jobs"]


def list_ids(client):
    return [e["job_id"] for e in listed(client)]


# ---------------------------------------------------------------------
# Protocol basics
# ---------------------------------------------------------------------

class TestProtocol:
    def test_initialize(self, server):
        res = server.initialize_response["result"]
        assert res["serverInfo"]["name"] == "mcp-exec"
        assert "tools" in res["capabilities"]
        assert res["protocolVersion"]

    def test_ping(self, server):
        assert server.request("ping").get("result") == {}

    def test_tools_list(self, server):
        tools = server.request("tools/list")["result"]["tools"]
        names = {t["name"] for t in tools}
        assert names == {"execute", "wait", "status", "output", "input", "kill", "list_jobs"}
        for t in tools:
            assert t["description"]
            assert t["inputSchema"]["type"] == "object"

    def test_unknown_method(self, server):
        assert server.request("bogus/method")["error"]["code"] == -32601

    def test_unknown_tool(self, server):
        msg = server.request("tools/call", {"name": "nope", "arguments": {}})
        assert msg["error"]["code"] == -32601
        assert "unknown tool" in msg["error"]["message"]

    def test_tools_call_invalid_params(self, server):
        assert server.request("tools/call", [1, 2, 3])["error"]["code"] == -32602

    def test_malformed_line_does_not_kill_server(self, server):
        server.send_raw("this is {not json\n")
        assert server.request("ping").get("result") == {}

    def test_string_request_ids(self, server):
        q = queue.Queue()
        with server._lock:
            server._pending["abc-1"] = q
        try:
            server._send({"jsonrpc": "2.0", "id": "abc-1", "method": "ping"})
            msg = q.get(timeout=10)
            assert msg is not None and msg.get("result") == {}
        finally:
            with server._lock:
                server._pending.pop("abc-1", None)


# ---------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------

class TestExecute:
    def test_echo_success(self, server):
        res = run(server, "echo hello-mcp")
        assert res["state"] == "success"
        assert res["exit_code"] == 0
        assert res["stdout"]["data"] == "hello-mcp\n"
        assert res.get("stderr") is None

    def test_error_captures_stderr_only_by_default(self, server):
        res = run(server, "echo out; echo boom >&2; exit 3")
        assert res["state"] == "error"
        assert res["exit_code"] == 3
        assert "boom" in res["stderr"]["data"]
        assert res.get("stdout") is None

    def test_success_captures_stdout_only_by_default(self, server):
        res = run(server, "echo ok; echo hidden >&2")
        assert res["stdout"]["data"] == "ok\n"
        assert res.get("stderr") is None

    def test_custom_capture_policy(self, server):
        res = run(server, "echo out; echo err >&2; exit 1", capture={"on_error": "all"})
        assert "out" in res["stdout"]["data"]
        assert "err" in res["stderr"]["data"]

    def test_invalid_capture_spec_rejected(self, server):
        err = server.tool_error("execute", {"command": "true", "capture": {"on_success": "bogus"}})
        assert "invalid" in err

    def test_cwd(self, server):
        res = run(server, "pwd", cwd="/tmp")
        assert res["stdout"]["data"].strip() in ("/tmp", "/private/tmp")

    def test_env_override_and_inherit(self, server):
        res = run(server, 'printf "%s" "$MCP_TEST_VAR"', env={"MCP_TEST_VAR": "hello-env"})
        assert res["stdout"]["data"] == "hello-env"
        res2 = run(server, 'echo "${PATH:+path-ok}"')
        assert res2["stdout"]["data"].strip() == "path-ok"

    def test_stdin_data_with_close(self, server):
        res = run(server, "cat", stdin="hello via stdin\n", close_stdin=True)
        assert res["state"] == "success"
        assert res["stdout"]["data"] == "hello via stdin\n"

    def test_start_failure_reaped(self, server):
        res = run(server, "echo never", cwd="/nonexistent-dir-xyz")
        assert res["state"] == "error"
        assert res["start_error"]
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("status", {"job_id": res["job_id"]})

    def test_start_failure_reaped_with_nowait(self, server):
        res = run(server, "echo never", cwd="/nonexistent-dir-xyz", nowait=True)
        assert res["state"] == "error"
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("status", {"job_id": res["job_id"]})

    def test_nowait_then_wait_reaps(self, server):
        job_id = spawn(server, "echo done-nowait")
        res = reap(server, job_id)
        assert res["state"] == "success"
        assert res["stdout"]["data"] == "done-nowait\n"

    def test_blocking_execute_reaps(self, server):
        res = run(server, "true")
        assert res["state"] == "success"
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("status", {"job_id": res["job_id"]})

    def test_backgrounded_child_does_not_block_wait(self, server):
        # Regression: a backgrounded descendant keeps the stdout pipe's
        # write end open; execute() must return as soon as the direct
        # child exits instead of blocking until EOF (5s here).
        t0 = time.monotonic()
        res = run(server, "echo hi; sleep 5 &")
        elapsed = time.monotonic() - t0
        assert res["state"] == "success"
        assert res["stdout"]["data"] == "hi\n"
        assert elapsed < 3

    def test_incomplete_last_line(self, server):
        res = run(server, "printf 'aaa\\nbbb'")
        assert res["stdout"]["data"] == "aaa\nbbb"
        assert res["stdout"]["total_bytes"] == 7


# ---------------------------------------------------------------------
# wait
# ---------------------------------------------------------------------

class TestWait:
    def test_wait_completes(self, server):
        job_id = spawn(server, "sleep 0.3; echo finished")
        results = server.tool("wait", {"job_ids": [job_id], "timeout": 10})
        assert len(results) == 1
        assert results[0]["state"] == "success"
        assert results[0]["stdout"]["data"] == "finished\n"

    def test_wait_timeout_reports_running(self, server):
        job_id = spawn(server, "sleep 30")
        try:
            t0 = time.monotonic()
            results = server.tool("wait", {"job_ids": [job_id], "timeout": 0.5})
            assert 0.3 < time.monotonic() - t0 < 5
            assert results[0]["state"] == "running"
            assert results[0].get("exit_code") is None
            assert server.tool("status", {"job_id": job_id})["state"] == "running"
        finally:
            cleanup_job(server, job_id)

    def test_wait_zero_timeout_is_an_instant_poll(self, server):
        job_id = spawn(server, "sleep 30")
        try:
            t0 = time.monotonic()
            results = server.tool("wait", {"job_ids": [job_id], "timeout": 0})
            assert time.monotonic() - t0 < 2
            assert results[0]["state"] == "running"
            # nothing delivered -> nothing reaped
            assert server.tool("status", {"job_id": job_id})["state"] == "running"
        finally:
            cleanup_job(server, job_id)

    def test_wait_zero_timeout_collects_ready_jobs(self, server):
        job_id = spawn(server, "echo ready")
        poll_terminal(server, job_id)
        results = server.tool("wait", {"job_ids": [job_id], "timeout": 0})
        assert results[0]["state"] == "success"
        assert results[0]["stdout"]["data"] == "ready\n"
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("status", {"job_id": job_id})

    def test_wait_default_timeout_from_env(self, server):
        job_id = spawn(server, "sleep 30")
        try:
            t0 = time.monotonic()
            results = server.tool("wait", {"job_ids": [job_id]}, timeout=15)
            assert 2.0 <= time.monotonic() - t0 <= 8.0  # MCP_EXEC_DEFAULT_TIMEOUT=3
            assert results[0]["state"] == "running"
        finally:
            cleanup_job(server, job_id)

    def test_wait_multiple_mixed(self, server):
        fast = spawn(server, "sleep 0.2; echo fast")
        slow = spawn(server, "sleep 30")
        try:
            results = server.tool("wait", {"job_ids": [fast, slow], "timeout": 10})
            by_id = {r["job_id"]: r for r in results}
            assert by_id[fast]["state"] == "success"
            assert by_id[slow]["state"] == "running"
            with pytest.raises(ToolError, match="unknown job_id"):
                server.tool("status", {"job_id": fast})
            assert server.tool("status", {"job_id": slow})["state"] == "running"
        finally:
            cleanup_job(server, slow)

    def test_wait_already_terminal_immediate(self, server):
        job_id = spawn(server, "echo quick")
        poll_terminal(server, job_id)
        t0 = time.monotonic()
        results = server.tool("wait", {"job_ids": [job_id], "timeout": 10})
        assert time.monotonic() - t0 < 2
        assert results[0]["state"] == "success"
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("status", {"job_id": job_id})

    def test_wait_duplicate_ids(self, server):
        job_id = spawn(server, "echo dup")
        results = server.tool("wait", {"job_ids": [job_id, job_id], "timeout": 10})
        assert len(results) == 2
        assert all(r["state"] == "success" for r in results)

    def test_wait_unknown_job(self, server):
        assert "unknown job_id" in server.tool_error("wait", {"job_ids": ["job-999999"]})

    def test_wait_empty_ids(self, server):
        assert "non-empty" in server.tool_error("wait", {"job_ids": []})


# ---------------------------------------------------------------------
# status / output
# ---------------------------------------------------------------------

class TestStatusOutput:
    def test_status_running(self, server):
        job_id = spawn(server, "sleep 30")
        try:
            st = server.tool("status", {"job_id": job_id})
            assert st["state"] == "running"
            assert st.get("exit_code") is None
        finally:
            cleanup_job(server, job_id)

    def test_status_unknown(self, server):
        assert "unknown job_id" in server.tool_error("status", {"job_id": "job-000000"})

    def test_status_does_not_reap(self, server):
        job_id = spawn(server, "echo keep")
        poll_terminal(server, job_id)
        for _ in range(3):
            assert server.tool("status", {"job_id": job_id})["state"] == "success"
        reap(server, job_id)

    def test_output_live_on_running_job(self, server):
        job_id = spawn(server, "echo early; sleep 30")
        try:
            def has_output():
                res = server.tool("output", {"job_id": job_id, "stdout": "all"})
                data = (res.get("stdout") or {}).get("data", "")
                return ("early" in data, None)
            wait_until(has_output, timeout=5)
            st = server.tool("output", {"job_id": job_id})
            assert st["state"] == "running"
            assert st.get("stdout") is None
        finally:
            cleanup_job(server, job_id)

    def test_output_tail_from_length(self, server):
        job_id = spawn(server, 'i=1; while [ $i -le 10 ]; do echo "line-$i"; i=$((i+1)); done; sleep 30')
        try:
            def all_lines():
                res = server.tool("output", {"job_id": job_id, "stdout": "all"})
                d = (res.get("stdout") or {}).get("data", "")
                return (d.count("\n") == 10, d)
            data = wait_until(all_lines, timeout=5)

            tail = server.tool("output", {"job_id": job_id, "stdout": {"tail": 3}})
            assert tail["stdout"]["data"] == "line-8\nline-9\nline-10\n"

            win = server.tool("output", {"job_id": job_id, "stdout": {"from": 6, "length": 12}})
            assert win["stdout"]["data"] == data[6:18]
            assert win["stdout"]["offset"] == 6

            r = server.tool("output", {"job_id": job_id, "stdout": {"from": -5}})
            assert r["stdout"]["data"] == data
            r = server.tool("output", {"job_id": job_id, "stdout": {"from": 0, "length": -1}})
            assert r["stdout"]["data"] == ""
        finally:
            cleanup_job(server, job_id)

    def test_output_none_omits_stream(self, server):
        job_id = spawn(server, "echo x")
        poll_terminal(server, job_id)
        try:
            res = server.tool("output", {"job_id": job_id, "stdout": "none", "stderr": "all"})
            assert res.get("stdout") is None
            assert res["stderr"]["data"] == ""
        finally:
            reap(server, job_id)

    def test_output_invalid_spec_rejected(self, server):
        assert "invalid" in server.tool_error("output", {"job_id": "whatever", "stdout": "bogus"})

    def test_output_on_terminal_before_reap_then_gone(self, server):
        job_id = spawn(server, "echo after-death")
        poll_terminal(server, job_id)
        res = server.tool("output", {"job_id": job_id, "stdout": "all"})
        assert res["stdout"]["data"] == "after-death\n"
        reap(server, job_id)
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("output", {"job_id": job_id, "stdout": "all"})

    def test_orphan_output_still_captured(self, server):
        job_id = spawn(server, "echo parent; (sleep 0.4; echo from-orphan) &")
        poll_terminal(server, job_id)  # must not block on the orphan
        def orphan_seen():
            res = server.tool("output", {"job_id": job_id, "stdout": "all"})
            data = (res.get("stdout") or {}).get("data", "")
            return ("parent" in data and "from-orphan" in data, None)
        wait_until(orphan_seen, timeout=5)
        reap(server, job_id)


# ---------------------------------------------------------------------
# Tail-window buffer semantics (MCP_EXEC_MAX_OUTPUT_BYTES=2048)
# ---------------------------------------------------------------------

class TestBufferWindow:
    LINES = [f"line-{i}-" + "x" * 24 + "\n" for i in range(100)]
    FULL = "".join(LINES)
    TOTAL = len(FULL)        # 3290
    MAX = 2048
    DROPPED = TOTAL - MAX    # 1242
    CMD = 'i=0; while [ $i -lt 100 ]; do echo "line-$i-xxxxxxxxxxxxxxxxxxxxxxxx"; i=$((i+1)); done'

    @pytest.fixture()
    def overflow_job(self, smallbuf):
        job_id = spawn(smallbuf, self.CMD)
        poll_terminal(smallbuf, job_id)
        yield job_id
        try:
            smallbuf.tool("wait", {"job_ids": [job_id], "timeout": 5})
        except ToolError:
            pass

    def test_constants_sanity(self, server):
        assert self.TOTAL == 3290 and self.DROPPED == 1242

    def test_all_keeps_tail_window(self, overflow_job, smallbuf):
        res = smallbuf.tool("output", {"job_id": overflow_job, "stdout": "all"})["stdout"]
        assert res["total_bytes"] == self.TOTAL
        assert len(res["data"]) == self.MAX
        assert res["data"] == self.FULL[-self.MAX:]
        assert res["truncated"] is True
        assert res["dropped_bytes"] == self.DROPPED
        assert res["offset"] == self.DROPPED

    def test_from_inside_truncated_head(self, overflow_job, smallbuf):
        frm = 100
        res = smallbuf.tool("output", {"job_id": overflow_job, "stdout": {"from": frm}})["stdout"]
        assert res["truncated"] is True
        assert res["dropped_bytes"] == self.DROPPED - frm
        assert res["offset"] == self.DROPPED
        assert res["data"] == self.FULL[self.DROPPED:]

    def test_from_zero_reports_full_head_loss(self, overflow_job, smallbuf):
        res = smallbuf.tool("output", {"job_id": overflow_job, "stdout": {"from": 0}})["stdout"]
        assert res["truncated"] is True
        assert res["dropped_bytes"] == self.DROPPED

    def test_from_inside_retained_window(self, overflow_job, smallbuf):
        frm, ln = self.DROPPED + 10, 64
        res = smallbuf.tool("output", {"job_id": overflow_job,
                                       "stdout": {"from": frm, "length": ln}})["stdout"]
        assert not res.get("truncated")
        assert res["offset"] == frm
        assert res["data"] == self.FULL[frm:frm + ln]

    def test_from_beyond_total(self, overflow_job, smallbuf):
        res = smallbuf.tool("output", {"job_id": overflow_job,
                                       "stdout": {"from": self.TOTAL + 100}})["stdout"]
        assert res["data"] == ""
        assert res["offset"] == self.TOTAL
        assert not res.get("truncated")

    def test_tail_more_lines_than_retained(self, overflow_job, smallbuf):
        res = smallbuf.tool("output", {"job_id": overflow_job, "stdout": {"tail": 10000}})["stdout"]
        assert res["data"] == self.FULL[-self.MAX:]
        assert res["truncated"] is True
        assert res["dropped_bytes"] == self.DROPPED

    def test_tail_few_lines_complete(self, overflow_job, smallbuf):
        res = smallbuf.tool("output", {"job_id": overflow_job, "stdout": {"tail": 2}})["stdout"]
        last_two = "".join(self.LINES[-2:])
        assert res["data"] == last_two
        assert not res.get("truncated")
        assert res["offset"] == self.TOTAL - len(last_two)

    def test_stderr_window_too(self, smallbuf):
        res = run(smallbuf, self.CMD + " >&2", capture={"on_success": "stderr"})
        assert res["state"] == "success"
        s = res["stderr"]
        assert s["total_bytes"] == self.TOTAL
        assert s["data"] == self.FULL[-self.MAX:]
        assert s["dropped_bytes"] == self.DROPPED
        assert s["offset"] == self.DROPPED

    def test_incremental_reads_track_stream(self, server):
        job_id = spawn(server,
                       'i=1; while [ $i -le 5 ]; do echo "chunk-$i"; sleep 0.3; i=$((i+1)); done; sleep 30')
        try:
            expected = "".join(f"chunk-{i}\n" for i in range(1, 6))
            got, pos = "", 0

            def step():
                nonlocal got, pos
                res = server.tool("output", {"job_id": job_id, "stdout": {"from": pos}})["stdout"]
                got += res["data"]
                pos = res.get("offset", pos) + len(res["data"])
                return (got == expected, got)

            wait_until(step, timeout=10, interval=0.15)
            assert got == expected
        finally:
            cleanup_job(server, job_id)


# ---------------------------------------------------------------------
# MB-scale output (window = 1 MiB, output = 3 MiB)
# ---------------------------------------------------------------------

class TestMegaByteOutput:
    LINE = "line-with-some-content\n"        # 23 bytes; not a divisor of TOTAL
    TOTAL = 3 * 1024 * 1024                 # 3145728
    WINDOW = 1024 * 1024                    # 1048576
    DROPPED = TOTAL - WINDOW
    FULL = (LINE * (TOTAL // len(LINE) + 1))[:TOTAL]
    CMD = f"yes '{LINE.rstrip()}' | head -c {TOTAL}"

    def test_window_keeps_exact_suffix(self, mbuf):
        job_id = spawn(mbuf, self.CMD)
        try:
            poll_terminal(mbuf, job_id, timeout=30)
            res = mbuf.tool("output", {"job_id": job_id, "stdout": "all"}, timeout=60)["stdout"]
            assert res["total_bytes"] == self.TOTAL
            assert res["dropped_bytes"] == self.DROPPED
            assert res["offset"] == self.DROPPED
            assert res["truncated"] is True
            assert len(res["data"]) == self.WINDOW
            assert res["data"] == self.FULL[-self.WINDOW:]   # byte-exact suffix
        finally:
            cleanup_job(mbuf, job_id)

    def test_blocking_execute_inline_megabyte_result(self, mbuf):
        # a ~1 MiB payload inside the execute response must survive the
        # stdio transport intact
        res = mbuf.tool("execute", {"command": self.CMD}, timeout=60)
        assert res["state"] == "success"
        assert res["stdout"]["total_bytes"] == self.TOTAL
        assert res["stdout"]["dropped_bytes"] == self.DROPPED
        assert res["stdout"]["data"] == self.FULL[-self.WINDOW:]

    def test_pagination_by_from_over_megabyte(self, mbuf):
        job_id = spawn(mbuf, self.CMD)
        try:
            poll_terminal(mbuf, job_id, timeout=30)
            chunk = 256 * 1024
            got, pos = "", self.DROPPED
            while pos < self.TOTAL:
                ln = min(chunk, self.TOTAL - pos)
                res = mbuf.tool("output",
                                {"job_id": job_id, "stdout": {"from": pos, "length": ln}},
                                timeout=60)["stdout"]
                assert res["offset"] == pos
                assert not res.get("truncated")
                got += res["data"]
                pos += len(res["data"])   # ASCII content: chars == bytes
                if not res["data"]:
                    break
            assert got == self.FULL[self.DROPPED:]
        finally:
            cleanup_job(mbuf, job_id)

    def test_from_inside_megabyte_truncated_head(self, mbuf):
        job_id = spawn(mbuf, self.CMD)
        try:
            poll_terminal(mbuf, job_id, timeout=30)
            frm = 1_000_000                      # < DROPPED (2097152)
            res = mbuf.tool("output", {"job_id": job_id, "stdout": {"from": frm}},
                            timeout=60)["stdout"]
            assert res["truncated"] is True
            assert res["dropped_bytes"] == self.DROPPED - frm
            assert res["offset"] == self.DROPPED
            assert res["data"] == self.FULL[self.DROPPED:]
        finally:
            cleanup_job(mbuf, job_id)


# ---------------------------------------------------------------------
# Binary torture
# ---------------------------------------------------------------------

class TestBinaryTorture:
    def test_large_binary_keeps_total_bytes(self, server):
        res = run(server, "head -c 65536 /dev/urandom")
        s = res["stdout"]
        assert s["total_bytes"] == 65536          # no byte lost to sanitizing
        assert "\ufffd" in s["data"]              # invalid UTF-8 replaced
        assert len(s["data"]) <= 65536            # each char consumes >= 1 byte

    def test_binary_roundtrip_via_base64(self, server):
        res = run(server, "head -c 4096 /dev/urandom | base64")
        decoded = base64.b64decode("".join(res["stdout"]["data"].split()))
        assert len(decoded) == 4096               # exact byte fidelity

    def test_nul_bytes_preserved(self, server):
        res = run(server, "head -c 16 /dev/zero")
        assert res["stdout"]["data"] == "\x00" * 16   # NUL is valid UTF-8
        assert res["stdout"]["total_bytes"] == 16

    def test_invalid_sequences_replaced_valid_survive(self, server):
        # \303\050 = invalid pair, \377 = invalid byte,
        # \346\227\245 = valid UTF-8 for 日 (must survive intact)
        res = run(server, r"printf 'A\303\050B\377C\346\227\245\n'")
        d = res["stdout"]["data"]
        assert d.startswith("A")
        assert "B" in d and "C" in d
        assert "\ufffd" in d
        assert "日" in d
        assert d.endswith("\n")

    def test_from_offsets_count_bytes_not_chars(self, server):
        # Invalid bytes isolated between valid ones: each FF becomes its
        # own U+FFFD regardless of whether the sanitizer replaces per
        # invalid byte or per invalid run, so chars map 1:1 to bytes here.
        job_id = spawn(server, r"printf '\377A\377B\377END\n'; sleep 10")
        try:
            def ready():
                res = server.tool("output", {"job_id": job_id, "stdout": "all"})
                return ("END" in (res.get("stdout") or {}).get("data", ""), None)
            wait_until(ready, timeout=5)

            # bytes: FF A FF B FF E N D \n  (9 bytes, indices 0..8)
            res = server.tool("output", {"job_id": job_id,
                                         "stdout": {"from": 3, "length": 4}})["stdout"]
            assert res["offset"] == 3
            assert res["data"] == "B\ufffdEN"          # bytes 3..6 = B FF E N

            res2 = server.tool("output", {"job_id": job_id, "stdout": {"from": 5}})["stdout"]
            assert res2["offset"] == 5
            assert res2["data"] == "END\n"             # bytes 5..8

            res3 = server.tool("output", {"job_id": job_id,
                                          "stdout": {"from": 0, "length": 5}})["stdout"]
            assert res3["offset"] == 0
            assert res3["data"] == "\ufffdA\ufffdB\ufffd"  # bytes 0..4 = FF A FF B FF
        finally:
            cleanup_job(server, job_id)

# ---------------------------------------------------------------------
# input & large stdin
# ---------------------------------------------------------------------

class TestInput:
    def test_input_roundtrip(self, server):
        job_id = spawn(server, "cat")
        try:
            server.tool("input", {"job_id": job_id, "data": "ping-1\n"})

            def sees():
                res = server.tool("output", {"job_id": job_id, "stdout": "all"})
                return ("ping-1" in (res.get("stdout") or {}).get("data", ""), None)
            wait_until(sees, timeout=5)

            server.tool("input", {"job_id": job_id, "data": "ping-2\n", "close": True})
            res = reap(server, job_id)
            assert res["state"] == "success"
            assert res["stdout"]["data"] == "ping-1\nping-2\n"
        finally:
            cleanup_job(server, job_id)

    def test_input_on_terminal_errors(self, server):
        job_id = spawn(server, "true")
        poll_terminal(server, job_id)
        assert "no longer running" in server.tool_error("input", {"job_id": job_id, "data": "x"})
        reap(server, job_id)

    def test_input_double_close_errors(self, server):
        # sleep keeps running after stdin closes, so the error is about
        # the closed stdin, not a dead job.
        job_id = spawn(server, "sleep 30")
        try:
            server.tool("input", {"job_id": job_id, "data": "", "close": True})
            err = server.tool_error("input", {"job_id": job_id, "data": "x"})
            assert "already closed" in err
        finally:
            cleanup_job(server, job_id)

    def test_input_unknown_job(self, server):
        assert "unknown job_id" in server.tool_error("input", {"job_id": "job-777777", "data": "x"})


class TestLargeStdin:
    def test_large_stdin_to_reader(self, server):
        res = run(server, "cat", stdin=BIG_STDIN, close_stdin=True)
        assert res["state"] == "success"
        assert len(res["stdout"]["data"]) == len(BIG_STDIN)

    def test_large_stdin_cancel_tears_down(self, server):
        # sleep never reads stdin: the 200KB write blocks on the full
        # pipe; cancelling must return promptly and tear the job down.
        baseline = set(list_ids(server))
        rid, q = server.request_async(
            "tools/call",
            {"name": "execute", "arguments": {"command": "sleep 30", "stdin": BIG_STDIN}})
        time.sleep(1.0)
        server.notify("notifications/cancelled", {"requestId": rid})
        msg = q.get(timeout=10)
        assert msg["error"]["code"] == -32800

        def cleaned():
            res = run(server, "pgrep -f 'sleep 3[0]' >/dev/null && echo FOUND || echo ABSENT")
            return (res["stdout"]["data"].strip() == "ABSENT"
                    and set(list_ids(server)) == baseline, None)
        wait_until(cleaned, timeout=15)

    def test_large_input_cancel_keeps_job(self, server):
        job_id = spawn(server, "sleep 30")
        try:
            rid, q = server.request_async(
                "tools/call",
                {"name": "input", "arguments": {"job_id": job_id, "data": BIG_STDIN}})
            time.sleep(1.0)
            server.notify("notifications/cancelled", {"requestId": rid})
            msg = q.get(timeout=10)
            assert msg["error"]["code"] == -32800
            assert server.tool("status", {"job_id": job_id})["state"] == "running"
        finally:
            cleanup_job(server, job_id)


# ---------------------------------------------------------------------
# kill
# ---------------------------------------------------------------------

class TestKill:
    def test_kill_term(self, server):
        job_id = spawn(server, "sleep 30")
        res = server.tool("kill", {"job_id": job_id})
        assert res["terminated"] is True
        assert res["signal_sent"] == 15
        assert "escalated" not in res
        assert res["result"]["state"] == "error"
        assert res["result"]["signal"] == "terminated"
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("status", {"job_id": job_id})

    def test_kill_number_nine(self, server):
        job_id = spawn(server, "sleep 30")
        res = server.tool("kill", {"job_id": job_id, "signal": "9"})
        assert res["terminated"] is True
        assert res["result"]["signal"] == "killed"
        assert "unknown job_id" in server.tool_error("status", {"job_id": job_id})

    def test_kill_invalid_signal(self, server):
        job_id = spawn(server, "sleep 30")
        try:
            assert "unknown signal" in server.tool_error("kill", {"job_id": job_id, "signal": "NOPE"})
        finally:
            cleanup_job(server, job_id)

    def test_kill_unknown_job(self, server):
        assert "unknown job_id" in server.tool_error("kill", {"job_id": "job-888888"})

    def test_kill_already_terminated_delivers_result(self, server):
        job_id = spawn(server, "echo late-result")
        poll_terminal(server, job_id)
        res = server.tool("kill", {"job_id": job_id})
        assert res["terminated"] is True
        assert res["already_terminated"] is True
        assert res["result"]["stdout"]["data"] == "late-result\n"
        assert "unknown job_id" in server.tool_error("status", {"job_id": job_id})

    def test_kill_term_survivor_then_assured(self, server):
        job_id = spawn(server, SURVIVOR)
        try:
            time.sleep(0.5)
            t0 = time.monotonic()
            res = server.tool("kill", {"job_id": job_id, "signal": "TERM", "timeout": 1}, timeout=10)
            assert time.monotonic() - t0 >= 0.9
            assert res["terminated"] is False
            assert res["state"] == "running"
            assert "detail" in res
            assert server.tool("status", {"job_id": job_id})["state"] == "running"
            res2 = server.tool("kill", {"job_id": job_id, "signal": "ASSURED", "timeout": 2}, timeout=15)
            assert res2["terminated"] is True
            assert res2["escalated"] is True
            assert res2["signal_sent"] == 9
            assert res2["result"]["signal"] == "killed"
        finally:
            cleanup_job(server, job_id)

    def test_kill_assured_no_escalation_when_term_suffices(self, server):
        job_id = spawn(server, "sleep 30")
        res = server.tool("kill", {"job_id": job_id, "signal": "ASSURED", "timeout": 3}, timeout=15)
        assert res["terminated"] is True
        assert "escalated" not in res
        assert res["signal_sent"] == 15

    def test_kill_timeout_zero_returns_fast(self, server):
        job_id = spawn(server, SURVIVOR)
        try:
            time.sleep(0.5)
            t0 = time.monotonic()
            res = server.tool("kill", {"job_id": job_id, "signal": "TERM", "timeout": 0})
            assert time.monotonic() - t0 < 2
            assert res["terminated"] is False
            res2 = server.tool("kill", {"job_id": job_id, "signal": "KILL", "timeout": 5})
            assert res2["terminated"] is True
        finally:
            cleanup_job(server, job_id)

    def test_kill_reaches_orphaned_grandchildren(self, server):
        job_id = spawn(server, "sleep 313 & echo started")
        poll_terminal(server, job_id)
        find = "pgrep -f 'sleep 31[3]' >/dev/null && echo FOUND || echo ABSENT"
        assert run(server, find)["stdout"]["data"].strip() == "FOUND"
        res = server.tool("kill", {"job_id": job_id, "signal": "TERM", "timeout": 5})
        assert res["terminated"] is True
        assert res["result"]["state"] == "success"
        assert run(server, find)["stdout"]["data"].strip() == "ABSENT"
        assert "unknown job_id" in server.tool_error("status", {"job_id": job_id})

    def test_externally_signaled_job_visible_via_wait(self, server):
        job_id = spawn(server, "sleep 30")
        killer = run(server, "pkill -TERM -f 'sleep 3[0]' && echo killed")
        assert killer["stdout"]["data"].strip() == "killed"
        results = server.tool("wait", {"job_ids": [job_id], "timeout": 5})
        assert results[0]["state"] == "error"
        assert results[0]["signal"] == "terminated"
        assert results[0]["exit_code"] == -1


# ---------------------------------------------------------------------
# list_jobs (all waitable jobs, with states)
# ---------------------------------------------------------------------

class TestListJobs:
    def test_lists_all_waitable_jobs_with_state(self, server):
        a = spawn(server, "sleep 30")
        b = spawn(server, "echo quick")
        try:
            poll_terminal(server, b)
            entries = {e["job_id"]: e["state"] for e in listed(server)}
            assert entries.get(a) == "running"
            assert entries.get(b) == "success"  # terminal, result uncollected
            ids = list_ids(server)
            assert ids.index(a) < ids.index(b)  # oldest first
        finally:
            cleanup_job(server, a)
            cleanup_job(server, b)

    def test_reaped_jobs_disappear(self, server):
        job_id = spawn(server, "echo bye")
        poll_terminal(server, job_id)
        assert job_id in list_ids(server)
        reap(server, job_id)
        assert job_id not in list_ids(server)

    def test_killed_running_job_disappears(self, server):
        job_id = spawn(server, "sleep 30")
        assert job_id in list_ids(server)
        cleanup_job(server, job_id)
        assert job_id not in list_ids(server)


# ---------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------

class TestCancellation:
    def test_cancel_blocking_execute_tears_down(self, server):
        baseline = set(list_ids(server))
        rid, q = server.request_async(
            "tools/call", {"name": "execute", "arguments": {"command": "sleep 30"}})
        time.sleep(0.7)
        server.notify("notifications/cancelled", {"requestId": rid})
        msg = q.get(timeout=10)
        assert msg["error"]["code"] == -32800

        def cleaned():
            res = run(server, "pgrep -f 'sleep 3[0]' >/dev/null && echo FOUND || echo ABSENT")
            return (res["stdout"]["data"].strip() == "ABSENT"
                    and set(list_ids(server)) == baseline, None)
        wait_until(cleaned, timeout=15)

    def test_cancel_wait_keeps_jobs(self, server):
        a = spawn(server, "sleep 30")
        b = spawn(server, "sleep 30")
        try:
            rid, q = server.request_async(
                "tools/call",
                {"name": "wait", "arguments": {"job_ids": [a, b], "timeout": 30}})
            time.sleep(0.5)
            server.notify("notifications/cancelled", {"requestId": rid})
            msg = q.get(timeout=10)
            assert msg["error"]["code"] == -32800
            assert server.tool("status", {"job_id": a})["state"] == "running"
            assert server.tool("status", {"job_id": b})["state"] == "running"
        finally:
            cleanup_job(server, a)
            cleanup_job(server, b)

    def test_cancel_kill(self, server):
        job_id = spawn(server, SURVIVOR)
        try:
            time.sleep(0.5)
            rid, q = server.request_async(
                "tools/call",
                {"name": "kill", "arguments": {"job_id": job_id, "signal": "ASSURED", "timeout": 10}})
            time.sleep(0.7)
            server.notify("notifications/cancelled", {"requestId": rid})
            msg = q.get(timeout=10)
            assert msg["error"]["code"] == -32800
            assert server.tool("status", {"job_id": job_id})["state"] == "running"
        finally:
            cleanup_job(server, job_id)

    def test_cancel_unknown_request_id_ignored(self, server):
        server.notify("notifications/cancelled", {"requestId": 987654})
        assert server.request("ping").get("result") == {}

    def test_late_cancel_ignored(self, server):
        rid, q = server.request_async("ping")
        assert q.get(timeout=10).get("result") == {}
        server.notify("notifications/cancelled", {"requestId": rid})
        assert server.request("ping").get("result") == {}

    def test_late_cancel_does_not_touch_other_requests(self, server):
        # request A finishes; a long request B is in flight; cancelling
        # the already-finished A must not disturb B
        rid_a, q_a = server.request_async(
            "tools/call", {"name": "execute", "arguments": {"command": "echo a"}})
        assert "result" in q_a.get(timeout=10)
        rid_b, q_b = server.request_async(
            "tools/call", {"name": "execute", "arguments": {"command": "sleep 1.5"}})
        time.sleep(0.3)
        server.notify("notifications/cancelled", {"requestId": rid_a})
        msg_b = q_b.get(timeout=15)
        assert "result" in msg_b, msg_b
        payload = json.loads(msg_b["result"]["content"][0]["text"])
        assert payload["state"] == "success"
        assert server.request("ping").get("result") == {}

    def test_cancel_id_type_mismatch_ignored(self, server):
        # requestId as a JSON string must NOT match an integer request id
        job_id = spawn(server, "sleep 30")
        rid, q = server.request_async(
            "tools/call", {"name": "wait", "arguments": {"job_ids": [job_id], "timeout": 30}})
        try:
            time.sleep(0.4)
            server.notify("notifications/cancelled", {"requestId": str(rid)})
            with pytest.raises(queue.Empty):
                q.get(timeout=1.0)          # still blocked -> mismatch ignored
            server.notify("notifications/cancelled", {"requestId": rid})
            assert q.get(timeout=10)["error"]["code"] == -32800
        finally:
            with server._lock:
                server._pending.pop(rid, None)
            cleanup_job(server, job_id)

    def test_double_cancel_harmless(self, server):
        job_id = spawn(server, "sleep 30")
        try:
            rid, q = server.request_async(
                "tools/call", {"name": "wait", "arguments": {"job_ids": [job_id], "timeout": 30}})
            time.sleep(0.4)
            server.notify("notifications/cancelled", {"requestId": rid})
            assert q.get(timeout=10)["error"]["code"] == -32800
            server.notify("notifications/cancelled", {"requestId": rid})   # again: no-op
            assert server.request("ping").get("result") == {}
            assert server.tool("status", {"job_id": job_id})["state"] == "running"
        finally:
            cleanup_job(server, job_id)


# ---------------------------------------------------------------------
# Progress notifications (MCP_EXEC_PROGRESS_INTERVAL=1 in the fixture)
# ---------------------------------------------------------------------

class TestProgress:
    def test_progress_notifications_emitted(self, server):
        server.drain_notifications()
        token = "tok-xyz"
        rid, q = server.request_async(
            "tools/call",
            {"name": "execute",
             "arguments": {"command": "sleep 2.5"},
             "_meta": {"progressToken": token}})
        msg = q.get(timeout=15)
        assert "result" in msg, msg
        payload = json.loads(msg["result"]["content"][0]["text"])
        assert payload["state"] == "success"
        notes = [n for n in server.drain_notifications()
                 if n.get("method") == "notifications/progress"
                 and n["params"]["progressToken"] == token]
        assert len(notes) >= 1
        nums = [n["params"]["progress"] for n in notes]
        assert nums == sorted(nums) and nums[0] >= 1
        assert all("message" in n["params"] for n in notes)

    def test_no_progress_without_token(self, server):
        server.drain_notifications()
        res = run(server, "sleep 1.5")
        assert res["state"] == "success"
        notes = [n for n in server.drain_notifications()
                 if n.get("method") == "notifications/progress"]
        assert notes == []

    def test_progress_on_long_wait(self, server):
        server.drain_notifications()
        job_id = spawn(server, "sleep 30")
        try:
            token = "tok-wait"
            rid, q = server.request_async(
                "tools/call",
                {"name": "wait",
                 "arguments": {"job_ids": [job_id], "timeout": 2.5},
                 "_meta": {"progressToken": token}})
            msg = q.get(timeout=15)
            assert "result" in msg, msg
            notes = [n for n in server.drain_notifications()
                     if n.get("method") == "notifications/progress"
                     and n["params"]["progressToken"] == token]
            assert len(notes) >= 1
        finally:
            cleanup_job(server, job_id)


# ---------------------------------------------------------------------
# PID-1 mode (no tini): the server must reap orphans itself
# ---------------------------------------------------------------------

class TestPid1:
    def test_basic_job_under_bare_pid1(self, bare_server):
        res = run(bare_server, "echo pid1-ok")
        assert res["state"] == "success"
        assert res["stdout"]["data"] == "pid1-ok\n"

    def test_exit_codes_intact_with_reaper(self, bare_server):
        # Rapid short jobs maximize the chance the reaper wins the
        # waitpid race; exit statuses must still come through intact.
        for code in (0, 3):
            res = run(bare_server, f"exit {code}")
            assert res["exit_code"] == code
        res = run(bare_server, "echo out; false")
        assert res["state"] == "error"
        assert res["exit_code"] == 1

    def test_reaper_drains_zombie_orphans(self, bare_server):
        # Without a reaper the dead orphan stays a zombie and keeps the
        # process group "alive", so kill()'s drain would time out.
        job_id = spawn(bare_server, "sleep 0.3 & echo go")
        poll_terminal(bare_server, job_id)
        time.sleep(1.0)  # the orphan is dead by now
        t0 = time.monotonic()
        res = bare_server.tool("kill", {"job_id": job_id, "signal": "TERM", "timeout": 4})
        assert res["terminated"] is True
        assert time.monotonic() - t0 < 3.0


# ---------------------------------------------------------------------
# Concurrency & misc
# ---------------------------------------------------------------------

class TestConcurrencyAndMisc:
    def test_blocking_call_does_not_block_others(self, server):
        rid, q = server.request_async(
            "tools/call", {"name": "execute", "arguments": {"command": "sleep 1.5"}})
        t0 = time.monotonic()
        assert server.request("ping").get("result") == {}
        names = {t["name"] for t in server.request("tools/list")["result"]["tools"]}
        assert "kill" in names
        server.tool("list_jobs")
        msg = q.get(timeout=15)
        payload = json.loads(msg["result"]["content"][0]["text"])
        assert payload["state"] == "success"
        assert time.monotonic() - t0 >= 1.0

    def test_invalid_utf8_sanitized(self, server):
        res = run(server, r"printf '\377\376ABC\n'")
        data = res["stdout"]["data"]
        assert "\ufffd" in data
        assert "ABC" in data

    def test_concurrent_kill_and_wait_no_crash(self, server):
        job_id = spawn(server, "sleep 0.4")
        rid, q = server.request_async(
            "tools/call", {"name": "wait", "arguments": {"job_ids": [job_id], "timeout": 10}})
        time.sleep(0.15)
        try:
            res = server.tool("kill", {"job_id": job_id, "signal": "KILL", "timeout": 5})
            assert res["terminated"] is True
        except ToolError:
            pass  # wait() may have delivered and reaped first
        msg = q.get(timeout=15)
        assert "result" in msg, msg
        payload = json.loads(msg["result"]["content"][0]["text"])
        assert payload[0]["job_id"] == job_id
        assert payload[0]["state"] == "error"  # killed by signal
        assert "unknown job_id" in server.tool_error("status", {"job_id": job_id})

    def test_kill_wait_race_sweep(self, server):
        # Sweep the race window across natural completion vs kill vs wait;
        # whatever interleaving wins, the outcome must be consistent and
        # the job must end up reaped.
        for delay in (0.05, 0.1, 0.2, 0.35, 0.5):
            job_id = spawn(server, f"sleep 0.4; echo done-{delay}")
            rid, q = server.request_async(
                "tools/call", {"name": "wait", "arguments": {"job_ids": [job_id], "timeout": 10}})
            time.sleep(delay)
            try:
                server.tool("kill", {"job_id": job_id, "signal": "ASSURED", "timeout": 3},
                            timeout=15)
            except ToolError as e:
                assert "unknown job_id" in str(e) or "no longer running" in str(e)
            msg = q.get(timeout=20)
            assert "result" in msg, msg
            payload = json.loads(msg["result"]["content"][0]["text"])
            assert payload[0]["job_id"] == job_id
            assert payload[0]["state"] in ("success", "error")
            with pytest.raises(ToolError, match="unknown job_id"):
                server.tool("status", {"job_id": job_id})

    def test_two_concurrent_kills_same_job(self, server):
        job_id = spawn(server, "sleep 30")
        rid1, q1 = server.request_async("tools/call", {
            "name": "kill", "arguments": {"job_id": job_id, "signal": "KILL", "timeout": 5}})
        rid2, q2 = server.request_async("tools/call", {
            "name": "kill", "arguments": {"job_id": job_id, "signal": "KILL", "timeout": 5}})
        successes = 0
        for q in (q1, q2):
            m = q.get(timeout=20)
            if "result" in m and not m["result"].get("isError"):
                payload = json.loads(m["result"]["content"][0]["text"])
                assert payload["terminated"] is True
                successes += 1
        assert successes >= 1        # typically 2; reap is idempotent
        with pytest.raises(ToolError, match="unknown job_id"):
            server.tool("status", {"job_id": job_id})
            
# ---------------------------------------------------------------------
# Graceful shutdown on stdin EOF
# ---------------------------------------------------------------------

class TestGracefulShutdown:
    def test_eof_cancels_inflight_and_exits(self, image):
        c = MCPClient(image)
        c.handshake()
        rid, q = c.request_async(
            "tools/call", {"name": "execute", "arguments": {"command": "sleep 30"}})
        time.sleep(0.7)          # the call is now blocking inside the server
        c.proc.stdin.close()     # client goes away
        try:
            msg = q.get(timeout=12)
        except queue.Empty:
            msg = None
        if msg is not None:
            # the cancelled call must be answered, not dropped silently
            assert msg.get("error", {}).get("code") == -32800
        # the server must exit promptly (bounded teardown), not hang on sleep 30
        assert c.proc.wait(timeout=15) is not None

    def test_eof_without_pending_requests_exits(self, image):
        c = MCPClient(image)
        c.handshake()
        res = run(c, "echo ok")
        assert res["state"] == "success"
        c.proc.stdin.close()
        assert c.proc.wait(timeout=10) is not None


# ---------------------------------------------------------------------
# UTF-8 character alignment of "from" reads
# ---------------------------------------------------------------------

class TestCharAlignment:
    # h é l l o →   with é = C3 A9 (2 bytes), → = E2 86 92 (3 bytes)
    TEXT = "héllo→"                       # 9 bytes total
    CMD = r"printf 'h\303\251llo\342\206\222'; sleep 10"

    @pytest.fixture()
    def text_job(self, server):
        job_id = spawn(server, self.CMD)
        def ready():
            res = server.tool("output", {"job_id": job_id, "stdout": "all"})
            return ((res.get("stdout") or {}).get("data") == self.TEXT, None)
        wait_until(ready, timeout=5)
        yield job_id
        cleanup_job(server, job_id)

    def test_from_inside_char_extends_back(self, text_job, server):
        # byte 2 is the second byte of é (bytes 1..2): read must start at é
        res = server.tool("output", {"job_id": text_job, "stdout": {"from": 2}})["stdout"]
        assert res["offset"] == 1
        assert res["data"] == "éllo→"

    def test_from_inside_three_byte_char(self, text_job, server):
        # bytes 7 and 8 are continuation bytes of → (bytes 6..8)
        for frm in (7, 8):
            res = server.tool("output", {"job_id": text_job, "stdout": {"from": frm}})["stdout"]
            assert res["offset"] == 6
            assert res["data"] == "→"

    def test_from_at_char_start_unchanged(self, text_job, server):
        res = server.tool("output", {"job_id": text_job, "stdout": {"from": 6}})["stdout"]
        assert res["offset"] == 6
        assert res["data"] == "→"

    def test_length_extends_to_char_end(self, text_job, server):
        # [0:2) would cut é in half -> extended to include all of é
        res = server.tool("output", {"job_id": text_job,
                                     "stdout": {"from": 0, "length": 2}})["stdout"]
        assert res["offset"] == 0
        assert res["data"] == "hé"

    def test_length_extends_over_whole_three_byte_char(self, text_job, server):
        # [0:7) lands inside → -> extended over the full character (all 9 bytes)
        res = server.tool("output", {"job_id": text_job,
                                     "stdout": {"from": 0, "length": 7}})["stdout"]
        assert res["data"] == self.TEXT

    def test_unaligned_true_gives_exact_bytes(self, text_job, server):
        # byte 2 alone is a dangling continuation byte -> one U+FFFD
        res = server.tool("output", {"job_id": text_job,
                                     "stdout": {"from": 2, "length": 3, "unaligned": True}})["stdout"]
        assert res["offset"] == 2
        assert res["data"] == "\ufffdll"

    def test_alignment_also_applies_in_capture_policy(self, server):
        # the same alignment applies to capture specs used by execute/wait
        res = run(server, r"printf 'h\303\251llo\342\206\222'",
                  capture={"on_success": {"stdout": {"from": 2}}})
        assert res["state"] == "success"
        assert res["stdout"]["offset"] == 1
        assert res["stdout"]["data"] == "éllo→"

    def test_bytes_field_closes_pagination(self, text_job, server):
        # walk the stream with from = offset + bytes; alignment must
        # never cause overlap, gaps, or drift
        got, pos = "", 0
        for _ in range(10):
            res = server.tool("output", {"job_id": text_job,
                                         "stdout": {"from": pos, "length": 2}})["stdout"]
            got += res["data"]
            if res["bytes"] == 0:
                break
            pos = res["offset"] + res["bytes"]
        assert got == self.TEXT
