// Command mcp-exec is a minimal stdio MCP server that exposes a shell
// command executor. It is deliberately NOT a terminal emulator and NOT a
// collection of specialized tools: it runs `$SHELL -c <command>` with
// ordinary pipes (no PTY), tracks each invocation as a job, and lets the
// LLM poll/wait for completion, pull output on demand, signal jobs, and
// list the jobs it can still wait on.
//
// Tools: execute, wait, status, output, input, kill, list_jobs.
//
// Job lifecycle: a job is deleted from the registry (and becomes
// inaccessible by any means) as soon as its terminal result has been
// captured and delivered by execute() or wait(), or by kill() once the
// signalled job has actually terminated. A signal is only a request and
// may be caught or ignored, so kill() observes actual death before
// cleaning up. list_jobs() shows every job still waitable: running jobs
// and terminal jobs whose result has not been collected yet.
//
// Process groups: every job's shell runs in its own process group
// (Setpgid). kill() signals the whole group and waits for the whole tree
// to drain, so backgrounded grandchildren don't survive as orphans
// holding ports and files. (Processes that intentionally call setsid()
// detach themselves from the group and are out of reach.)
//
// PID-1 operation: when running as PID 1 (container without a separate
// init) the server reaps orphaned descendants itself; otherwise their
// zombies would keep process groups "alive" and block kill()'s drain.
// Exit statuses of job children are routed correctly even when the
// reaper wins the waitpid race against cmd.Wait.
//
// Output buffering: each stream keeps a sliding window of the LAST
// maxOutputBytes bytes. When output overflows the window the oldest bytes
// are dropped and counted. All read offsets ("from") are absolute,
// measured from the very start of the stream, in BYTES; a read that
// starts inside the dropped head region reports how many of the requested
// bytes were truncated and serves from the oldest byte still retained.
// Captured data is UTF-8 sanitized before being embedded in JSON (invalid
// bytes -> U+FFFD).
//
// Character alignment: "from" reads are aligned outward to UTF-8
// character boundaries by default, so multi-byte characters are never
// split: if the start position points inside a character the read extends
// back to that character's first byte, and if the end position points
// inside a character the read extends forward past its last byte. Set
// "unaligned": true in the stream-capture spec for exact byte slicing.
//
// Cancellation: requests carrying an id can be cancelled by the client
// via notifications/cancelled. The blocking tools (execute, wait, kill)
// observe cancellation promptly and answer with JSON-RPC error -32800. A
// cancelled blocking execute() tears down the job it started, since the
// caller never received its job_id and the job would be unreachable.
//
// Shutdown: when stdin reaches EOF (client gone), every in-flight request
// is cancelled (blocking tools answer -32800 and return, tearing down any
// jobs whose ids never reached the client), and the process exits promptly
// — the wait for outstanding teardowns is bounded.
//
// Progress: when a tools/call request carries _meta.progressToken, long
// blocking operations (execute, wait, kill) emit notifications/progress
// every MCP_EXEC_PROGRESS_INTERVAL seconds while they block.
//
// Transport: JSON-RPC 2.0 over stdio, newline-delimited (one JSON value
// per line), per the MCP stdio transport convention. No HTTP is
// implemented here; if HTTP is needed it should be added by wrapping this
// stdio process externally (e.g. an mcp-proxy in front of it).
//
// Environment-level knobs:
//
//	MCP_EXEC_DEFAULT_TIMEOUT    default wait() timeout in seconds (float, default 60)
//	MCP_EXEC_MAX_OUTPUT_BYTES   max bytes retained per stream per job (default 1<<20)
//	MCP_EXEC_PROGRESS_INTERVAL  seconds between progress notifications (float, default 10)
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"
)

// ---------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------

type config struct {
	defaultTimeout   time.Duration
	maxOutputBytes   int
	progressInterval time.Duration
	shell            string
}

func loadConfig() config {
	cfg := config{
		defaultTimeout:   60 * time.Second,
		maxOutputBytes:   1 << 20, // 1 MiB
		progressInterval: 10 * time.Second,
	}
	if v := os.Getenv("MCP_EXEC_DEFAULT_TIMEOUT"); v != "" {
		if secs, err := strconv.ParseFloat(v, 64); err == nil && secs > 0 {
			cfg.defaultTimeout = time.Duration(secs * float64(time.Second))
		}
	}
	if v := os.Getenv("MCP_EXEC_MAX_OUTPUT_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.maxOutputBytes = n
		}
	}
	if v := os.Getenv("MCP_EXEC_PROGRESS_INTERVAL"); v != "" {
		if secs, err := strconv.ParseFloat(v, 64); err == nil && secs > 0 {
			cfg.progressInterval = time.Duration(secs * float64(time.Second))
		}
	}
	cfg.shell = os.Getenv("SHELL")
	if cfg.shell == "" {
		cfg.shell = "/bin/sh"
	}
	return cfg
}

// ---------------------------------------------------------------------
// Bounded output capture buffer (sliding tail window)
// ---------------------------------------------------------------------

// capBuffer is an io.Writer that retains the LAST `max` bytes of everything
// written to it (a sliding tail window). Bytes that fall out of the head of
// the window are dropped but counted in `dropped`, so readers can keep
// using absolute "from" offsets (measured from the very start of the
// stream) and learn how many of the bytes they asked for were truncated.
// The writer never reports an error or short write to its caller, since a
// full capture buffer must not stall or break the process writing to it.
type capBuffer struct {
	mu      sync.Mutex
	buf     []byte
	max     int
	dropped int64 // bytes truncated from the head of the stream
	total   int64 // total bytes ever written (== dropped + len(buf))
}

func newCapBuffer(max int) *capBuffer {
	if max <= 0 {
		max = 1 << 20
	}
	return &capBuffer{max: max}
}

func (b *capBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.total += int64(len(p))
	if len(p) >= b.max {
		b.buf = append(b.buf[:0], p[len(p)-b.max:]...)
		b.dropped = b.total - int64(b.max)
		return len(p), nil
	}
	b.buf = append(b.buf, p...)
	if overflow := len(b.buf) - b.max; overflow > 0 {
		b.buf = append(b.buf[:0], b.buf[overflow:]...)
		b.dropped += int64(overflow)
	}
	return len(p), nil
}

// snapshot returns a copy of the retained window plus the number of bytes
// dropped from the head and the total number of bytes ever written. The
// retained window always covers the absolute byte range [dropped, total).
func (b *capBuffer) snapshot() (data []byte, dropped int64, total int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]byte, len(b.buf))
	copy(out, b.buf)
	return out, b.dropped, b.total
}

// ---------------------------------------------------------------------
// Capture specs
// ---------------------------------------------------------------------

// streamCapture describes what slice of a single stream's captured buffer
// to return. Zero value is "none". For "from" reads, Unaligned=false (the
// default) aligns the byte range outward to UTF-8 character boundaries;
// Unaligned=true requests exact byte slicing.
type streamCapture struct {
	mode      string // "none", "all", "tail", "from"
	tail      int
	from      int
	length    int
	hasLen    bool
	unaligned bool
}

func (s *streamCapture) UnmarshalJSON(data []byte) error {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "null" {
		*s = streamCapture{mode: "none"}
		return nil
	}
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "all":
			*s = streamCapture{mode: "all"}
		case "none", "":
			*s = streamCapture{mode: "none"}
		default:
			return fmt.Errorf("invalid stream capture string %q (want \"all\" or \"none\")", str)
		}
		return nil
	}
	var obj struct {
		Tail      *int  `json:"tail"`
		From      *int  `json:"from"`
		Length    *int  `json:"length"`
		Unaligned *bool `json:"unaligned"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("invalid stream capture spec: %w", err)
	}
	switch {
	case obj.Tail != nil:
		*s = streamCapture{mode: "tail", tail: *obj.Tail}
	case obj.From != nil:
		sc := streamCapture{mode: "from", from: *obj.From}
		if obj.Length != nil {
			sc.length = *obj.Length
			sc.hasLen = true
		}
		sc.unaligned = obj.Unaligned != nil && *obj.Unaligned
		*s = sc
	default:
		return errors.New("stream capture object must set \"tail\" or \"from\"")
	}
	return nil
}

// captureSpec bundles a stdout + stderr streamCapture. It accepts either
// the compact string shorthand ("stdout" | "stderr" | "all" | "none") or
// the explicit {"stdout": ..., "stderr": ...} object form.
type captureSpec struct {
	Stdout streamCapture
	Stderr streamCapture
}

func (c *captureSpec) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		switch str {
		case "stdout":
			*c = captureSpec{Stdout: streamCapture{mode: "all"}, Stderr: streamCapture{mode: "none"}}
		case "stderr":
			*c = captureSpec{Stdout: streamCapture{mode: "none"}, Stderr: streamCapture{mode: "all"}}
		case "all":
			*c = captureSpec{Stdout: streamCapture{mode: "all"}, Stderr: streamCapture{mode: "all"}}
		case "none", "":
			*c = captureSpec{Stdout: streamCapture{mode: "none"}, Stderr: streamCapture{mode: "none"}}
		default:
			return fmt.Errorf("invalid capture shorthand %q", str)
		}
		return nil
	}
	var obj struct {
		Stdout *streamCapture `json:"stdout"`
		Stderr *streamCapture `json:"stderr"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("invalid capture spec: %w", err)
	}
	if obj.Stdout != nil {
		c.Stdout = *obj.Stdout
	} else {
		c.Stdout = streamCapture{mode: "none"}
	}
	if obj.Stderr != nil {
		c.Stderr = *obj.Stderr
	} else {
		c.Stderr = streamCapture{mode: "none"}
	}
	return nil
}

// capturePolicy is the on_success/on_error pair supplied to execute. It
// travels with the job so wait()/execute() never have to ask the LLM what
// to return once a job completes.
type capturePolicy struct {
	OnSuccess *captureSpec `json:"on_success,omitempty"`
	OnError   *captureSpec `json:"on_error,omitempty"`
}

func defaultCapturePolicy() *capturePolicy {
	return &capturePolicy{
		OnSuccess: &captureSpec{Stdout: streamCapture{mode: "all"}, Stderr: streamCapture{mode: "none"}},
		OnError:   &captureSpec{Stdout: streamCapture{mode: "none"}, Stderr: streamCapture{mode: "all"}},
	}
}

// captureResult is the rendered slice of a stream's buffer returned to the
// caller. All offsets are absolute BYTE offsets, measured from the very
// start of the stream; Offset is always present and points at the first
// returned byte. If the requested slice reaches into bytes that were
// already truncated from the head of the capture window, Truncated is set
// and DroppedBytes reports how many of the requested bytes are gone.
type captureResult struct {
	Data         string `json:"data"`
	Truncated    bool   `json:"truncated,omitempty"`
	DroppedBytes int64  `json:"dropped_bytes,omitempty"`
	TotalBytes   int64  `json:"total_bytes,omitempty"`
	Offset       int64  `json:"offset"` // always emitted: pagination must never guess
	Bytes        int64  `json:"bytes"`  // bytes actually returned; next_from = offset + bytes
}

// toSafeText turns raw captured bytes into a string that is always safe to
// embed in JSON: sequences that are not valid UTF-8 are replaced with
// U+FFFD so marshaling can never fail or silently mangle the payload.
func toSafeText(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return strings.ToValidUTF8(string(b), "\uFFFD")
}

// isUTF8Continuation reports whether b is a UTF-8 continuation byte
// (10xxxxxx).
func isUTF8Continuation(b byte) bool {
	return b&0xC0 == 0x80
}

// alignToRunes expands the byte range [rel, end) outward to UTF-8
// character boundaries within data: if rel points inside a multi-byte
// character it moves back to that character's first byte (never below 0),
// and if end points inside a character it moves forward past that
// character's last byte (never beyond len(data)). A character already cut
// off at the window head simply starts at the window head.
func alignToRunes(data []byte, rel, end int64) (int64, int64) {
	n := int64(len(data))
	if rel >= n {
		return n, n
	}
	for rel > 0 && isUTF8Continuation(data[rel]) {
		rel--
	}
	for end < n && isUTF8Continuation(data[end]) {
		end++
	}
	if end < rel {
		end = rel
	}
	return rel, end
}

// renderCapture applies a streamCapture spec to a capBuffer's current
// contents. It is safe to call while the job is still running. "from"
// offsets are always absolute (from the start of the stream), regardless
// of what the capture window currently retains.
func renderCapture(buf *capBuffer, spec streamCapture) *captureResult {
	if spec.mode == "" || spec.mode == "none" {
		return nil
	}
	data, dropped, total := buf.snapshot()
	switch spec.mode {
	case "all":
		return &captureResult{
			Data:         toSafeText(data),
			Truncated:    dropped > 0,
			DroppedBytes: dropped,
			TotalBytes:   total,
			Offset:       dropped,
			Bytes:        int64(len(data)),
		}
	case "tail":
		n := spec.tail
		if n < 0 {
			n = 0
		}
		lines := splitLinesKeepEnds(data)
		start := 0
		if n < len(lines) {
			start = len(lines) - n
		}
		rel := 0
		for _, ln := range lines[:start] {
			rel += len(ln)
		}
		res := &captureResult{
			Data:       toSafeText(data[rel:]),
			TotalBytes: total,
			Offset:     dropped + int64(rel),
			Bytes:      int64(len(data) - rel),
		}
		if start == 0 && dropped > 0 {
			res.Truncated = true
			res.DroppedBytes = dropped
		}
		return res
	case "from":
		from := int64(spec.from)
		if from < 0 {
			from = 0
		}
		if from > total {
			from = total
		}
		res := &captureResult{TotalBytes: total}
		if from < dropped {
			res.Truncated = true
			res.DroppedBytes = dropped - from
			from = dropped
		}
		rel := from - dropped
		end := int64(len(data))
		if spec.hasLen {
			l := int64(spec.length)
			if l < 0 {
				l = 0
			}
			if rel+l < end {
				end = rel + l
			}
		}
		if !spec.unaligned {
			// Default: never split a UTF-8 character. A start inside a
			// character extends the read back to the character's first
			// byte; an end inside a character extends it forward past
			// the character's last byte (both bounded by the retained
			// window). Unaligned=true restores exact byte slicing.
			rel, end = alignToRunes(data, rel, end)
		}
		res.Offset = dropped + rel
		res.Bytes = end - rel          // post-alignment end
		res.Data = toSafeText(data[rel:end])
		return res
	}
	return nil
}

// splitLinesKeepEnds splits data into lines, retaining the trailing "\n" on
// every line but the (possibly incomplete) last one, so re-joining
// reproduces the original bytes exactly.
func splitLinesKeepEnds(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	var lines []string
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, string(data[start:i+1]))
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, string(data[start:]))
	}
	return lines
}

// ---------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------

type jobState string

const (
	stateRunning jobState = "running"
	stateSuccess jobState = "success"
	stateError   jobState = "error"
)

type job struct {
	ID      string
	Command string

	mu        sync.Mutex
	state     jobState
	exitCode  int
	signal    string
	startErr  string
	startedAt time.Time
	endedAt   time.Time

	// stolenCh/stolenWS carry the exit status of the direct child when
	// the PID-1 orphan reaper wins the waitpid race against cmd.Wait.
	stolenCh chan struct{}
	stolenWS syscall.WaitStatus

	stdout  *capBuffer
	stderr  *capBuffer
	capture *capturePolicy

	cmd         *exec.Cmd
	stdinMu     sync.Mutex
	stdinW      io.WriteCloser
	stdinClosed bool

	done chan struct{}
}

func (j *job) snapshotState() (state jobState, exitCode int, signal, startErr string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.state, j.exitCode, j.signal, j.startErr
}

func (j *job) isTerminal() bool {
	st, _, _, _ := j.snapshotState()
	return st != stateRunning
}

// childReaped reports (non-blocking) whether the direct child has been
// waited for, i.e. whether j.done has been closed.
func childReaped(j *job) bool {
	select {
	case <-j.done:
		return true
	default:
		return false
	}
}

// sendGroupSignal delivers sig to the job's entire process group (the
// shell and everything it spawned). A negative pid addresses the group.
// ESRCH (group already gone) is reported as os.ErrProcessDone.
func (j *job) sendGroupSignal(sig syscall.Signal) error {
	if j.cmd == nil || j.cmd.Process == nil {
		return errors.New("job has no process")
	}
	err := syscall.Kill(-j.cmd.Process.Pid, sig)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

// groupAlive reports whether any member of the job's process group is
// still present (signal-0 probe).
func (j *job) groupAlive() bool {
	if j.cmd == nil || j.cmd.Process == nil {
		return false
	}
	return !errors.Is(syscall.Kill(-j.cmd.Process.Pid, 0), syscall.ESRCH)
}

// recordStolenStatus stores the exit status captured by the PID-1 reaper.
func (j *job) recordStolenStatus(ws syscall.WaitStatus) {
	j.mu.Lock()
	defer j.mu.Unlock()
	select {
	case <-j.stolenCh:
		return
	default:
	}
	j.stolenWS = ws
	close(j.stolenCh)
}

// stolenOutcome retrieves that status (bounded wait; the reaper stores it
// right after winning the waitpid race, so this returns almost instantly).
func (j *job) stolenOutcome() childOutcome {
	select {
	case <-j.stolenCh:
		j.mu.Lock()
		ws := j.stolenWS
		j.mu.Unlock()
		return outcomeFromWaitStatus(ws)
	case <-time.After(5 * time.Second):
		return childOutcome{exitCode: -1, startErr: "exit status lost: child reaped before its pid was registered"}
	}
}

// activeCaptureSpec returns the capture spec that applies given the job's
// current (terminal) state: on_success if it exited 0, on_error otherwise.
func (j *job) activeCaptureSpec() *captureSpec {
	st, _, _, _ := j.snapshotState()
	if j.capture == nil {
		j.capture = defaultCapturePolicy()
	}
	if st == stateSuccess {
		if j.capture.OnSuccess != nil {
			return j.capture.OnSuccess
		}
		return &captureSpec{}
	}
	if j.capture.OnError != nil {
		return j.capture.OnError
	}
	return &captureSpec{}
}

// startErrPresent reports whether the job failed to start (vs. ran and
// exited non-zero).
func (j *job) startErrPresent() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.startErr != "" && j.exitCode == -1 && j.signal == ""
}

// childOutcome is the resolved fate of a job's direct child.
type childOutcome struct {
	exitCode int
	signal   string
	startErr string
}

func (o childOutcome) success() bool {
	return o.exitCode == 0 && o.signal == "" && o.startErr == ""
}

func outcomeFromWaitError(waitErr error) childOutcome {
	switch e := waitErr.(type) {
	case nil:
		return childOutcome{}
	case *exec.ExitError:
		if ws, ok := e.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return childOutcome{exitCode: -1, signal: ws.Signal().String()}
		}
		return childOutcome{exitCode: e.ExitCode()}
	default:
		return childOutcome{exitCode: -1, startErr: waitErr.Error()}
	}
}

func outcomeFromWaitStatus(ws syscall.WaitStatus) childOutcome {
	if ws.Signaled() {
		return childOutcome{exitCode: -1, signal: ws.Signal().String()}
	}
	return childOutcome{exitCode: ws.ExitStatus()}
}

// ---------------------------------------------------------------------
// Job manager
// ---------------------------------------------------------------------

type manager struct {
	cfg    config
	mu     sync.Mutex
	jobs   map[string]*job
	nextID uint64

	pidMu sync.Mutex
	byPid map[int]*job

	teardownWG sync.WaitGroup
}

func newManager(cfg config) *manager {
	return &manager{cfg: cfg, jobs: make(map[string]*job), byPid: make(map[int]*job)}
}

// installOrphanReaper matters only when we run as PID 1 (a container
// without a separate init): orphaned descendants of job shells get
// reparented to us, and left alone they would become zombies that keep
// their process group "alive" for kill(-pgid, 0), so kill()'s group drain
// would never complete. We therefore reap them here. waitpid(-1) can also
// win the race for a job shell's own exit against cmd.Wait's waitpid; in
// that case the status is routed to the owning job instead of being lost.
func (m *manager) installOrphanReaper() {
	if os.Getpid() != 1 {
		return
	}
	sigCh := make(chan os.Signal, 32)
	signal.Notify(sigCh, syscall.SIGCHLD)
	go func() {
		reap := func() {
			for {
				var ws syscall.WaitStatus
				pid, err := syscall.Wait4(-1, &ws, syscall.WNOHANG, nil)
				if pid <= 0 || err != nil {
					return
				}
				if j := m.jobByPid(pid); j != nil {
					j.recordStolenStatus(ws)
				}
				// anything else is an orphaned grandchild: reaped, done.
			}
		}
		reap()
		for range sigCh {
			reap()
		}
	}()
}

func (m *manager) trackPid(j *job) {
	if j.cmd == nil || j.cmd.Process == nil {
		return
	}
	m.pidMu.Lock()
	defer m.pidMu.Unlock()
	m.byPid[j.cmd.Process.Pid] = j
}

func (m *manager) untrackPid(pid int) {
	m.pidMu.Lock()
	defer m.pidMu.Unlock()
	delete(m.byPid, pid)
}

func (m *manager) jobByPid(pid int) *job {
	m.pidMu.Lock()
	defer m.pidMu.Unlock()
	return m.byPid[pid]
}

// mergedEnv layers `overrides` on top of the server process's own
// environment: inherited variables are kept unless a key in overrides
// replaces them, and new keys are appended.
func mergedEnv(overrides map[string]string) []string {
	base := os.Environ()
	merged := make(map[string]string, len(base)+len(overrides))
	for _, kv := range base {
		if i := strings.IndexByte(kv, '='); i >= 0 {
			merged[kv[:i]] = kv[i+1:]
		}
	}
	for k, v := range overrides {
		merged[k] = v
	}
	result := make([]string, 0, len(merged))
	for k, v := range merged {
		result = append(result, k+"="+v)
	}
	return result
}

func (m *manager) newJobID() string {
	n := atomic.AddUint64(&m.nextID, 1)
	return fmt.Sprintf("job-%d", n)
}

func (m *manager) register(j *job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
}

func (m *manager) get(id string) (*job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return nil, fmt.Errorf("unknown job_id %q", id)
	}
	return j, nil
}

// remove deletes a job from the registry, making it inaccessible by any
// means. Called when a terminal result has been delivered by execute()/
// wait(), or by kill() once the signalled job actually terminated.
// Deleting is idempotent.
func (m *manager) remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
}

// writeStdin is the single stdin-writing path shared by execute's initial
// stdin, the input tool, and the best-effort close on job termination.
func writeStdin(j *job, data string, closeAfter bool) error {
	j.stdinMu.Lock()
	defer j.stdinMu.Unlock()
	if j.stdinClosed {
		return errors.New("stdin is already closed for this job")
	}
	if data != "" {
		if _, err := io.WriteString(j.stdinW, data); err != nil {
			return fmt.Errorf("writing stdin: %w", err)
		}
	}
	if closeAfter {
		j.stdinClosed = true
		if err := j.stdinW.Close(); err != nil {
			return fmt.Errorf("closing stdin: %w", err)
		}
	}
	return nil
}

// writeStdinCancellable runs writeStdin in a worker so a cancelled request
// cannot get stuck behind a blocked pipe write (e.g. stdin larger than the
// kernel pipe buffer given to a child that never reads). On cancellation
// the worker is left to finish on its own: it keeps holding stdinMu, which
// is correct since stdin writes are strictly serialized, and it unblocks
// with EPIPE once the job's process tree is killed (or the child drains
// the pipe). Note a cancelled close=true still takes effect as soon as the
// pending write finishes.
func writeStdinCancellable(ctx context.Context, j *job, data string, closeAfter bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ch := make(chan error, 1)
	go func() {
		ch <- writeStdin(j, data, closeAfter)
	}()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// startJob starts `command` as a new job; see the package doc for the
// process-group, capture-pipe and PID-1 design notes.
func (m *manager) startJob(command, cwd string, env map[string]string, capture *capturePolicy) *job {
	if capture == nil {
		capture = defaultCapturePolicy()
	}
	cmd := exec.Command(m.cfg.shell, "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if cwd != "" {
		cmd.Dir = cwd
	}
	if len(env) > 0 {
		cmd.Env = mergedEnv(env)
	}
	// StdinPipe hands the child a raw fd: cmd.Wait() then does not block
	// on stdin EOF for jobs that never read.
	stdinW, stdinErr := cmd.StdinPipe()
	// Capture stdout/stderr through pipes we own ourselves rather than
	// handing the capBuffers to os/exec: with exec-owned copies cmd.Wait()
	// blocks until EOF, which never happens while a backgrounded
	// descendant keeps a write end open ("daemon &"). Owning the pipes
	// lets cmd.Wait() return as soon as the direct child exits, while the
	// reader goroutines keep capturing output from surviving descendants.
	outR, outW, outErr := os.Pipe()
	var errR, errW *os.File
	var errErr error
	if outErr == nil {
		errR, errW, errErr = os.Pipe()
	}
	j := &job{
		ID:        m.newJobID(),
		Command:   command,
		state:     stateRunning,
		exitCode:  -1,
		startedAt: time.Now(),
		stolenCh:  make(chan struct{}),
		stdout:    newCapBuffer(m.cfg.maxOutputBytes),
		stderr:    newCapBuffer(m.cfg.maxOutputBytes),
		capture:   capture,
		cmd:       cmd,
		stdinW:    stdinW,
		done:      make(chan struct{}),
	}
	m.register(j)
	if setupErr := firstNonNil(stdinErr, outErr, errErr); setupErr != nil {
		closeAll(stdinW, outR, outW, errR, errW)
		j.mu.Lock()
		j.state = stateError
		j.startErr = fmt.Sprintf("setting up pipes: %v", setupErr)
		j.endedAt = time.Now()
		j.mu.Unlock()
		close(j.done)
		return j
	}
	cmd.Stdout = outW
	cmd.Stderr = errW
	if err := cmd.Start(); err != nil {
		closeAll(stdinW, outR, outW, errR, errW)
		j.mu.Lock()
		j.state = stateError
		j.startErr = err.Error()
		j.endedAt = time.Now()
		j.mu.Unlock()
		close(j.done)
		return j
	}
	m.trackPid(j)
	// Parent never writes to these ends: close so EOF arrives once the
	// child and every fd-inheriting descendant have exited.
	outW.Close()
	errW.Close()
	// Readers intentionally outlive cmd.Wait(): descendants that inherited
	// the pipes keep feeding the capture buffers.
	outDone := make(chan struct{})
	errDone := make(chan struct{})
	go func() { io.Copy(j.stdout, outR); outR.Close(); close(outDone) }()
	go func() { io.Copy(j.stderr, errR); errR.Close(); close(errDone) }()
	go func() {
		pid := cmd.Process.Pid
		waitErr := cmd.Wait()
		var oc childOutcome
		if waitErr != nil && errors.Is(waitErr, syscall.ECHILD) {
			// The PID-1 reaper won the waitpid race for this child;
			// it stored the exit status for us.
			oc = j.stolenOutcome()
		} else {
			oc = outcomeFromWaitError(waitErr)
		}
		// Settle in-flight bytes before the terminal state becomes
		// observable, so terminal results don't lose the child's tail.
		settleCapture(j, outDone, errDone)
		j.mu.Lock()
		j.endedAt = time.Now()
		if oc.success() {
			j.state = stateSuccess
		} else {
			j.state = stateError
		}
		j.exitCode = oc.exitCode
		j.signal = oc.signal
		if oc.startErr != "" {
			j.startErr = oc.startErr
		}
		j.mu.Unlock()
		_ = writeStdin(j, "", true)
		m.untrackPid(pid)
		close(j.done)
	}()
	return j
}

// teardown makes a best-effort ensured kill of j (SIGTERM to the whole
// process group, escalating to SIGKILL if it survives) and reaps the job
// record. Used when a blocking execute() is cancelled before it could
// report the job_id.
func (m *manager) teardown(j *job) {
	if childReaped(j) && !j.groupAlive() {
		m.remove(j.ID)
		return
	}
	if j.cmd != nil && j.cmd.Process != nil {
		_ = j.sendGroupSignal(syscall.SIGTERM)
		if !waitGone(context.Background(), j, defaultKillGrace, nil) {
			_ = j.sendGroupSignal(syscall.SIGKILL)
			waitGone(context.Background(), j, defaultKillGrace, nil)
		}
	}
	m.remove(j.ID)
}

// teardownAsync runs teardown in the background, tracked so graceful
// shutdown can wait (bounded) for all of them.
func (m *manager) teardownAsync(j *job) {
	m.teardownWG.Add(1)
	go func() {
		defer m.teardownWG.Done()
		m.teardown(j)
	}()
}

// awaitTeardowns waits for outstanding teardowns, bounded by limit so a
// TERM-resistant tree cannot hang shutdown.
func (m *manager) awaitTeardowns(limit time.Duration) {
	done := make(chan struct{})
	go func() {
		m.teardownWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(limit):
	}
}

func firstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func closeAll(files ...interface{ Close() error }) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}

// ---------------------------------------------------------------------
// Result rendering shared by execute / wait / status / output
// ---------------------------------------------------------------------

type jobResult struct {
	JobID      string         `json:"job_id"`
	State      jobState       `json:"state"`
	ExitCode   *int           `json:"exit_code,omitempty"`
	Signal     string         `json:"signal,omitempty"`
	StartError string         `json:"start_error,omitempty"`
	Command    string         `json:"command,omitempty"`
	Stdout     *captureResult `json:"stdout,omitempty"`
	Stderr     *captureResult `json:"stderr,omitempty"`
}

// statusResult renders state only (no output).
func statusResult(j *job) *jobResult {
	st, exitCode, signal, startErr := j.snapshotState()
	r := &jobResult{JobID: j.ID, State: st, Signal: signal, StartError: startErr}
	if st != stateRunning {
		ec := exitCode
		r.ExitCode = &ec
	}
	return r
}

// terminalResult renders state plus captured output selected by the job's
// capture policy. Only valid to call once the job is terminal.
func terminalResult(j *job) *jobResult {
	r := statusResult(j)
	spec := j.activeCaptureSpec()
	r.Stdout = renderCapture(j.stdout, spec.Stdout)
	r.Stderr = renderCapture(j.stderr, spec.Stderr)
	return r
}

// explicitOutputResult renders state plus whatever stdout/stderr slices
// were explicitly requested, independent of capture policy.
func explicitOutputResult(j *job, stdout, stderr *streamCapture) *jobResult {
	r := statusResult(j)
	if stdout != nil {
		r.Stdout = renderCapture(j.stdout, *stdout)
	}
	if stderr != nil {
		r.Stderr = renderCapture(j.stderr, *stderr)
	}
	return r
}

// waitResults renders each job and REAPS every job whose terminal result
// is captured here: a terminal result is delivered exactly once. The
// single isTerminal check drives both the render style and the deletion.
func (m *manager) waitResults(jobs []*job) []*jobResult {
	results := make([]*jobResult, 0, len(jobs))
	for _, j := range jobs {
		if j.isTerminal() {
			results = append(results, terminalResult(j))
			m.remove(j.ID)
		} else {
			results = append(results, statusResult(j))
		}
	}
	return results
}

// ---------------------------------------------------------------------
// Blocking support: progress, kill, settling
// ---------------------------------------------------------------------

// progressReporter emits notifications/progress (MCP) while a tool call
// blocks. All methods are nil/token-safe no-ops when no progress token was
// supplied by the client.
type progressReporter struct {
	srv   *server
	token json.RawMessage
	every time.Duration
	n     int
}

func (p *progressReporter) emit(message string) {
	if p == nil || len(p.token) == 0 {
		return
	}
	p.n++
	p.srv.send(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "notifications/progress",
		"params": map[string]interface{}{
			"progressToken": json.RawMessage(p.token),
			"progress":      p.n,
			"message":       message,
		},
	})
}

// blockUntil waits for any of: done closes (true, nil), ctx cancelled
// (false, ctx.Err()), timeoutCh fires (false, nil; a nil timeoutCh never
// fires). Emits progress notifications every rep.every while blocking.
func blockUntil(ctx context.Context, done <-chan struct{}, timeoutCh <-chan time.Time, rep *progressReporter, what string) (bool, error) {
	var tickCh <-chan time.Time
	if rep != nil && len(rep.token) > 0 && rep.every > 0 {
		ticker := time.NewTicker(rep.every)
		defer ticker.Stop()
		tickCh = ticker.C
	}
	for {
		select {
		case <-done:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timeoutCh:
			return false, nil
		case <-tickCh:
			rep.emit(what)
		}
	}
}

// defaultKillGrace bounds how long kill() waits after each signal for the
// process tree to actually die before escalating (or giving up).
const defaultKillGrace = 5 * time.Second

// signalAssured is a pseudo-signal accepted by kill(): "ensured kill" —
// SIGTERM first, escalating to SIGKILL if the tree survives the grace.
const signalAssured = "ASSURED"

// parseKillSignals accepts "" (default SIGTERM), a signal number ("9"), a
// signal name with or without the SIG prefix (case-insensitive), or the
// pseudo-signal ASSURED, and returns the ordered list of signals to try.
func parseKillSignals(s string) ([]syscall.Signal, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return []syscall.Signal{syscall.SIGTERM}, nil
	}
	name := strings.ToUpper(s)
	name = strings.TrimPrefix(name, "SIG")
	if name == signalAssured {
		return []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL}, nil
	}
	if n, err := strconv.Atoi(s); err == nil {
		if n <= 0 {
			return nil, fmt.Errorf("invalid signal number %d", n)
		}
		return []syscall.Signal{syscall.Signal(n)}, nil
	}
	if sig, ok := signalNames[name]; ok {
		return []syscall.Signal{sig}, nil
	}
	return nil, fmt.Errorf("unknown signal %q (use e.g. \"TERM\", \"KILL\", \"ASSURED\", or a number)", s)
}

// signalNames maps short uppercase names (without the SIG prefix) to
// signals that exist on both Linux and macOS.
var signalNames = map[string]syscall.Signal{
	"HUP": syscall.SIGHUP, "INT": syscall.SIGINT, "QUIT": syscall.SIGQUIT,
	"ILL": syscall.SIGILL, "TRAP": syscall.SIGTRAP, "ABRT": syscall.SIGABRT,
	"BUS": syscall.SIGBUS, "FPE": syscall.SIGFPE, "KILL": syscall.SIGKILL,
	"SEGV": syscall.SIGSEGV, "PIPE": syscall.SIGPIPE, "ALRM": syscall.SIGALRM,
	"TERM": syscall.SIGTERM, "USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2,
	"CHLD": syscall.SIGCHLD, "CONT": syscall.SIGCONT, "STOP": syscall.SIGSTOP,
	"TSTP": syscall.SIGTSTP, "TTIN": syscall.SIGTTIN, "TTOU": syscall.SIGTTOU,
	"URG": syscall.SIGURG, "XCPU": syscall.SIGXCPU, "XFSZ": syscall.SIGXFSZ,
	"VTALRM": syscall.SIGVTALRM, "PROF": syscall.SIGPROF, "WINCH": syscall.SIGWINCH,
}

// waitGone waits until the job's process tree is fully gone: direct child
// reaped AND no process-group member left. grace bounds the total wait;
// grace <= 0 only checks the current state. Returns false early on ctx
// cancellation.
func waitGone(ctx context.Context, j *job, grace time.Duration, rep *progressReporter) bool {
	if grace <= 0 {
		return childReaped(j) && !j.groupAlive()
	}
	deadline := time.Now().Add(grace)
	if !waitChildReaped(ctx, j, deadline, rep) {
		return false
	}
	var nextProgress time.Time
	if rep != nil {
		nextProgress = time.Now().Add(rep.every)
	}
	for j.groupAlive() {
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		if rep != nil && time.Now().After(nextProgress) {
			rep.emit("waiting for the process group to drain")
			nextProgress = nextProgress.Add(rep.every)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// waitChildReaped waits for j.done bounded by deadline/ctx, emitting
// progress notes along the way.
func waitChildReaped(ctx context.Context, j *job, deadline time.Time, rep *progressReporter) bool {
	var nextProgress time.Time
	if rep != nil {
		nextProgress = time.Now().Add(rep.every)
	}
	for {
		if childReaped(j) {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		slice := 100 * time.Millisecond
		if remaining < slice {
			slice = remaining
		}
		timer := time.NewTimer(slice)
		select {
		case <-j.done:
			timer.Stop()
			return true
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if rep != nil && time.Now().After(nextProgress) {
			rep.emit("waiting for the process to exit")
			nextProgress = nextProgress.Add(rep.every)
		}
	}
}

// settleCapture waits until the capture buffers stop growing (EOF in the
// common case; two stable samples when a surviving descendant still holds
// a pipe), bounded by a hard deadline.
func settleCapture(j *job, outDone, errDone <-chan struct{}) {
	const (
		pollGap   = 10 * time.Millisecond
		maxSettle = 500 * time.Millisecond
	)
	deadline := time.Now().Add(maxSettle)
	prevOut, prevErr := int64(-1), int64(-1)
	for {
		if chanClosed(outDone) && chanClosed(errDone) {
			return
		}
		_, _, totOut := j.stdout.snapshot()
		_, _, totErr := j.stderr.snapshot()
		if prevOut >= 0 && totOut == prevOut && totErr == prevErr {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		prevOut, prevErr = totOut, totErr
		time.Sleep(pollGap)
	}
}

func chanClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------
// Tool argument types
// ---------------------------------------------------------------------

type executeArgs struct {
	Command    string            `json:"command"`
	Cwd        string            `json:"cwd,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Stdin      *string           `json:"stdin,omitempty"`
	CloseStdin bool              `json:"close_stdin,omitempty"`
	NoWait     bool              `json:"nowait,omitempty"`
	Capture    *capturePolicy    `json:"capture,omitempty"`
}

type waitArgs struct {
	JobIDs  []string `json:"job_ids"`
	Timeout *float64 `json:"timeout,omitempty"`
}

type statusArgs struct {
	JobID string `json:"job_id"`
}

type outputArgs struct {
	JobID  string         `json:"job_id"`
	Stdout *streamCapture `json:"stdout,omitempty"`
	Stderr *streamCapture `json:"stderr,omitempty"`
}

type inputArgs struct {
	JobID string `json:"job_id"`
	Data  string `json:"data"`
	Close bool   `json:"close,omitempty"`
}

type killArgs struct {
	JobID   string   `json:"job_id"`
	Signal  string   `json:"signal,omitempty"`
	Timeout *float64 `json:"timeout,omitempty"` // seconds to wait for actual death after each signal
}

// ---------------------------------------------------------------------
// Tool implementations
// ---------------------------------------------------------------------

func (m *manager) toolExecute(ctx context.Context, raw json.RawMessage, rep *progressReporter) (interface{}, error) {
	var args executeArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if strings.TrimSpace(args.Command) == "" {
		return nil, errors.New("\"command\" is required")
	}
	j := m.startJob(args.Command, args.Cwd, args.Env, args.Capture)
	// A failure to start is returned immediately with fail status,
	// regardless of nowait. The terminal result is captured right here,
	// so the job is reaped before returning and is no longer accessible.
	if j.isTerminal() && j.startErrPresent() {
		r := terminalResult(j)
		m.remove(j.ID)
		return r, nil
	}
	if args.Stdin != nil || args.CloseStdin {
		data := ""
		if args.Stdin != nil {
			data = *args.Stdin
		}
		if err := writeStdinCancellable(ctx, j, data, args.CloseStdin); err != nil {
			if ctx.Err() != nil {
				// Cancelled before the job_id could be reported: the
				// job would be unreachable — tear it down.
				m.teardownAsync(j)
				return nil, ctx.Err()
			}
			// Real stdin error; the job is alive and queryable.
			r := statusResult(j)
			return map[string]interface{}{
				"job_id":      j.ID,
				"state":       r.State,
				"stdin_error": err.Error(),
			}, nil
		}
	}
	if args.NoWait {
		return map[string]interface{}{"job_id": j.ID, "state": "running"}, nil
	}
	// No nowait: execute behaves like wait([job]) with no timeout.
	if _, err := blockUntil(ctx, j.done, nil, rep, "command still running"); err != nil {
		m.teardownAsync(j)
		return nil, err
	}
	r := terminalResult(j)
	// Terminal result captured and delivered: reap the job.
	m.remove(j.ID)
	return r, nil
}

func (m *manager) toolWait(ctx context.Context, raw json.RawMessage, rep *progressReporter) (interface{}, error) {
	var args waitArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if len(args.JobIDs) == 0 {
		return nil, errors.New("\"job_ids\" must be a non-empty array")
	}
	jobs := make([]*job, 0, len(args.JobIDs))
	for _, id := range args.JobIDs {
		j, err := m.get(id)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	// Already-finished jobs: return immediately without waiting at all.
	if anyTerminal(jobs) {
		return m.waitResults(jobs), nil
	}
	timeout := m.cfg.defaultTimeout
	if args.Timeout != nil {
		timeout = time.Duration(*args.Timeout * float64(time.Second))
	}
	if timeout <= 0 {
		// timeout <= 0 means "don't block": report current states,
		// collecting (and reaping) whatever is already terminal. A poll.
		return m.waitResults(jobs), nil
	}
	notify := make(chan struct{}, len(jobs))
	for _, j := range jobs {
		// ctx in the select: no goroutine outlives the request.
		go func(j *job) {
			select {
			case <-j.done:
				select {
				case notify <- struct{}{}:
				default:
				}
			case <-ctx.Done():
			}
		}(j)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	if _, err := blockUntil(ctx, notify, timer.C, rep, "waiting for jobs"); err != nil {
		return nil, err
	}
	return m.waitResults(jobs), nil
}

func anyTerminal(jobs []*job) bool {
	for _, j := range jobs {
		if j.isTerminal() {
			return true
		}
	}
	return false
}

func (m *manager) toolStatus(_ context.Context, raw json.RawMessage) (interface{}, error) {
	var args statusArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	j, err := m.get(args.JobID)
	if err != nil {
		return nil, err
	}
	return statusResult(j), nil
}

func (m *manager) toolOutput(_ context.Context, raw json.RawMessage) (interface{}, error) {
	var args outputArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	j, err := m.get(args.JobID)
	if err != nil {
		return nil, err
	}
	// output(job_id) with neither stream requested is equivalent to status.
	return explicitOutputResult(j, args.Stdout, args.Stderr), nil
}

func (m *manager) toolInput(ctx context.Context, raw json.RawMessage) (interface{}, error) {
	var args inputArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if args.JobID == "" {
		return nil, errors.New("\"job_id\" is required")
	}
	j, err := m.get(args.JobID)
	if err != nil {
		return nil, err
	}
	if j.isTerminal() {
		return nil, fmt.Errorf("job %q is no longer running", args.JobID)
	}
	if err := writeStdinCancellable(ctx, j, args.Data, args.Close); err != nil {
		return nil, err
	}
	return statusResult(j), nil
}

func (m *manager) toolKill(ctx context.Context, raw json.RawMessage, rep *progressReporter) (interface{}, error) {
	var args killArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, fmt.Errorf("invalid arguments: %w", err)
	}
	if args.JobID == "" {
		return nil, errors.New("\"job_id\" is required")
	}
	j, err := m.get(args.JobID)
	if err != nil {
		return nil, err
	}
	signals, err := parseKillSignals(args.Signal)
	if err != nil {
		return nil, err
	}
	// Fast path: the direct child is reaped AND nothing survives in its
	// process group — the job is fully gone. Deliver the (never yet
	// delivered) terminal result and reap.
	if childReaped(j) && !j.groupAlive() {
		r := terminalResult(j)
		m.remove(j.ID)
		return map[string]interface{}{
			"job_id":             j.ID,
			"terminated":         true,
			"already_terminated": true,
			"result":             r,
		}, nil
	}
	if j.cmd == nil || j.cmd.Process == nil {
		return nil, fmt.Errorf("job %q has no running process", args.JobID)
	}
	grace := defaultKillGrace
	if args.Timeout != nil {
		grace = time.Duration(*args.Timeout * float64(time.Second))
	}
	// A signal is only a request: the process may catch or ignore it.
	// Cleanup must not key off delivery — wait for the whole process
	// group to actually die after each signal. ASSURED escalates to
	// SIGKILL if the tree survives SIGTERM.
	escalated := false
	var lastSig syscall.Signal
	for i, sig := range signals {
		lastSig = sig
		if i > 0 {
			escalated = true
		}
		if err := j.sendGroupSignal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return nil, fmt.Errorf("signaling job %q: %w", args.JobID, err)
		}
		if waitGone(ctx, j, grace, rep) {
			// Actually dead, whole tree gone: deliver the terminal
			// result (once) and reap the job.
			r := terminalResult(j)
			m.remove(j.ID)
			res := map[string]interface{}{
				"job_id":      j.ID,
				"terminated":  true,
				"signal_sent": int(lastSig),
				"result":      r,
			}
			if escalated {
				res["escalated"] = true
			}
			return res, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	// Every signal in the plan was delivered and the tree is still (at
	// least partially) alive. Leave the job accessible.
	res := map[string]interface{}{
		"job_id":      j.ID,
		"terminated":  false,
		"signal_sent": int(lastSig),
	}
	if escalated {
		res["escalated"] = true
	}
	if !childReaped(j) {
		res["state"] = "running"
		res["detail"] = "signal delivered but the process is still running (it may catch or ignore the signal); retry with KILL/9 or ASSURED"
	} else {
		st, _, _, _ := j.snapshotState()
		res["state"] = st
		res["detail"] = "the direct process died but other processes from its process group are still alive; retry with ASSURED or KILL/9"
	}
	return res, nil
}

type jobListItem struct {
	JobID string   `json:"job_id"`
	State jobState `json:"state"`
}

// toolListJobs lists every job the server still tracks — running jobs and
// terminal jobs whose result has not been collected yet. Every listed job
// is waitable: wait() delivers the terminal result and deletes the job.
func (m *manager) toolListJobs(_ context.Context, _ json.RawMessage) (interface{}, error) {
	m.mu.Lock()
	all := make([]*job, 0, len(m.jobs))
	for _, j := range m.jobs {
		all = append(all, j)
	}
	m.mu.Unlock()
	sort.Slice(all, func(i, k int) bool {
		if !all[i].startedAt.Equal(all[k].startedAt) {
			return all[i].startedAt.Before(all[k].startedAt)
		}
		return all[i].ID < all[k].ID
	})
	entries := make([]jobListItem, 0, len(all))
	for _, j := range all {
		st, _, _, _ := j.snapshotState()
		entries = append(entries, jobListItem{JobID: j.ID, State: st})
	}
	return map[string]interface{}{"jobs": entries}, nil
}

// ---------------------------------------------------------------------
// MCP / JSON-RPC plumbing
// ---------------------------------------------------------------------

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

const (
	codeParseError       = -32700
	codeInvalidRequest   = -32600
	codeMethodNotFound   = -32601
	codeInvalidParams    = -32602
	codeInternalError    = -32603
	codeRequestCancelled = -32800 // per MCP convention for cancelled requests
)

type toolDef struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

func toolDefs() []toolDef {
	streamCaptureSchema := map[string]interface{}{
		"description": `"none" (omit), "all" (the whole retained window), {"tail": N} (last N lines), ` +
			`{"from": P} (from absolute byte offset P, measured from the very start of the stream), ` +
			`or {"from": P, "length": N}. "from" reads are UTF-8 character-aligned by default: if P or ` +
			`P+length lands inside a multi-byte character, the read is extended so the whole character ` +
			`is included — the start extends back to the character's first byte, the end extends forward ` +
			`past its last byte; set "unaligned": true for exact byte slicing. If the requested range ` +
			`reaches into bytes already truncated from the head, the reply sets "truncated" and reports ` +
			`the missing count in "dropped_bytes", serving from the oldest retained byte.`,
		"oneOf": []interface{}{
			map[string]interface{}{"type": "string", "enum": []string{"all", "none"}},
			map[string]interface{}{
				"type":                 "object",
				"properties":           map[string]interface{}{"tail": map[string]interface{}{"type": "integer"}},
				"required":             []string{"tail"},
				"additionalProperties": false,
			},
			map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"from":   map[string]interface{}{"type": "integer"},
					"length": map[string]interface{}{"type": "integer"},
					"unaligned": map[string]interface{}{
						"type":        "boolean",
						"description": "true = exact byte slicing; false/omitted = extend from/length outward to UTF-8 character boundaries so no character is split.",
					},
				},
				"required":             []string{"from"},
				"additionalProperties": false,
			},
		},
	}
	captureSpecSchema := map[string]interface{}{
		"description": `"stdout", "stderr", "all", "none", or {"stdout": <stream-capture>, "stderr": <stream-capture>}.`,
		"oneOf": []interface{}{
			map[string]interface{}{"type": "string", "enum": []string{"stdout", "stderr", "all", "none"}},
			map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"stdout": streamCaptureSchema,
					"stderr": streamCaptureSchema,
				},
				"additionalProperties": false,
			},
		},
	}
	capturePolicySchema := map[string]interface{}{
		"type":        "object",
		"description": "What to capture depending on the job's eventual outcome. Default: on_success -> stdout, on_error -> stderr.",
		"properties": map[string]interface{}{
			"on_success": captureSpecSchema,
			"on_error":   captureSpecSchema,
		},
		"additionalProperties": false,
	}
	return []toolDef{
		{
			Name: "execute",
			Description: "Run a shell command ($SHELL -c <command>, no PTY) as a job. " +
				"Without nowait, blocks until the job finishes (equivalent to wait([job])) and returns " +
				"the terminal result. With nowait, returns the job_id as soon as the process has started " +
				"(a start failure is still returned immediately as a fail result). " +
				"Whenever this call returns a terminal result (success or start failure), the job is deleted " +
				"immediately afterwards and is no longer accessible: the result is delivered exactly once. " +
				"Honours request cancellation; a cancelled blocking call tears down the job it started. " +
				"Emits notifications/progress while blocking if the request carries _meta.progressToken. " +
				"stdin is written before the call returns (cancellable); keep it small for children that " +
				"never read stdin — a large write blocks until the process drains it or dies. Capture " +
				"specs with \"from\" are character-aligned by default (see the stream-capture schema).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"command": map[string]interface{}{"type": "string", "description": "Shell command line to run."},
					"cwd":     map[string]interface{}{"type": "string", "description": "Working directory for the process. Defaults to the server's own cwd."},
					"env": map[string]interface{}{
						"type":                 "object",
						"description":          "Environment variables to set/override, layered on top of the server's own environment (not a full replacement).",
						"additionalProperties": map[string]interface{}{"type": "string"},
					},
					"stdin":       map[string]interface{}{"type": "string", "description": "Optional data to write to the process's stdin immediately."},
					"close_stdin": map[string]interface{}{"type": "boolean", "description": "Close stdin (EOF) after writing the optional stdin data. Default false: stdin stays open for later input() calls."},
					"nowait":      map[string]interface{}{"type": "boolean", "description": "Return job_id immediately instead of blocking for completion."},
					"capture":     capturePolicySchema,
				},
				"required":             []string{"command"},
				"additionalProperties": false,
			},
		},
		{
			Name: "wait",
			Description: "Block until any one of the given jobs moves from running to a terminal state, " +
				"or until timeout (seconds) elapses. timeout <= 0 does not block at all: it reports current " +
				"states and collects (deletes) whatever is already terminal — use it as a poll. Returns the " +
				"current result for every requested job as an array in the same order as job_ids: terminal jobs " +
				"include captured output per their capture policy, still-running jobs report state only. Jobs " +
				"whose terminal result is included in the response are deleted immediately afterwards: a " +
				"terminal result is delivered exactly once. Honours request cancellation and emits " +
				"notifications/progress when a progressToken is supplied.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_ids": map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "string"}, "minItems": 1},
					"timeout": map[string]interface{}{"type": "number", "description": "Seconds. Defaults to the server's configured default timeout. <= 0: instant poll."},
				},
				"required":             []string{"job_ids"},
				"additionalProperties": false,
			},
		},
		{
			Name: "status",
			Description: "Return just the state of a job (running/success/error), with no output captured. " +
				"Never deletes the job. Returns unknown job_id for jobs already reaped (terminal result " +
				"delivered by execute/wait, or actually terminated by kill).",
			InputSchema: map[string]interface{}{
				"type":                 "object",
				"properties":           map[string]interface{}{"job_id": map[string]interface{}{"type": "string"}},
				"required":             []string{"job_id"},
				"additionalProperties": false,
			},
		},
		{
			Name: "output",
			Description: "Fetch stdout/stderr slices from a job's captured buffers, live or after completion. " +
				"Each stream keeps only the last N bytes (see dropped_bytes/truncated when the head was lost). " +
				"\"from\" offsets are always absolute BYTE offsets from the start of the stream, and reads are " +
				"aligned outward to UTF-8 character boundaries by default so multi-byte characters are never " +
				"split (use \"unaligned\": true for exact byte slicing). Captured data is UTF-8 sanitized " +
				"(invalid bytes become U+FFFD). Independent of the job's capture policy; never deletes the job. " +
				"output(job_id) with neither stream requested is equivalent to status(job_id). Returns unknown " +
				"job_id for jobs already reaped.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
					"stdout": streamCaptureSchema,
					"stderr": streamCaptureSchema,
				},
				"required":             []string{"job_id"},
				"additionalProperties": false,
			},
		},
		{
			Name:        "input",
			Description: "Write data to a running job's stdin. close=true writes the data then closes stdin (EOF). Honours request cancellation (a cancelled large write keeps the job alive).",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
					"data":   map[string]interface{}{"type": "string"},
					"close":  map[string]interface{}{"type": "boolean"},
				},
				"required":             []string{"job_id"},
				"additionalProperties": false,
			},
		},
		{
			Name: "kill",
			Description: "Signal a running job. The signal can be a name (\"TERM\", \"KILL\", \"SIGINT\", ...), " +
				"a number (\"9\"), or the pseudo-signal \"ASSURED\" for an ensured kill: first sends SIGTERM " +
				"and, if the process is still alive after the wait, escalates to SIGKILL. Signals are delivered " +
				"to the job's entire process group (the shell and everything it spawned), and this call waits up " +
				"to timeout seconds (default 5; 0 = don't wait) after each signal for the whole tree to actually " +
				"die — a signal is only a request and may be caught or ignored. If the tree terminates, the job's " +
				"terminal result is returned and the job is deleted; if it survives, terminated=false is reported " +
				"and the job stays accessible. A job whose tree is already fully gone is delivered and deleted " +
				"without signalling; a job whose shell already exited but which still has surviving child " +
				"processes is still signalled. Honours request cancellation and progress tokens.",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"job_id": map[string]interface{}{"type": "string"},
					"signal": map[string]interface{}{
						"type": "string",
						"description": "Signal name (with or without SIG prefix), number, or \"ASSURED\" " +
							"(ensured kill: SIGTERM, escalating to SIGKILL if the tree survives the wait). " +
							"Defaults to TERM.",
					},
					"timeout": map[string]interface{}{
						"type": "number",
						"description": "Seconds to wait for the process tree to actually die after each signal " +
							"delivery (ASSURED may deliver two). Defaults to 5. 0 means return right after delivery.",
					},
				},
				"required":             []string{"job_id"},
				"additionalProperties": false,
			},
		},
		{
			Name: "list_jobs",
			Description: "List every job the server still tracks: running jobs and finished jobs whose result " +
				"has not been collected yet. Every listed job is waitable — wait() returns the terminal result " +
				"and deletes the job. Entries carry job_id and state, oldest first.",
			InputSchema: map[string]interface{}{
				"type":                 "object",
				"properties":           map[string]interface{}{},
				"additionalProperties": false,
			},
		},
	}
}

type server struct {
	mgr   *manager
	outMu sync.Mutex
	out   *bufio.Writer

	reqMu    sync.Mutex
	inflight map[string]context.CancelFunc
}

func newServer(mgr *manager, w io.Writer) *server {
	return &server{mgr: mgr, out: bufio.NewWriter(w), inflight: make(map[string]context.CancelFunc)}
}

func (s *server) send(v interface{}) {
	s.outMu.Lock()
	defer s.outMu.Unlock()
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	s.out.Write(b)
	s.out.WriteByte('\n')
	s.out.Flush()
}

func (s *server) sendResult(id json.RawMessage, result interface{}) {
	s.send(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *server) sendError(id json.RawMessage, code int, msg string) {
	s.send(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}})
}

// track registers the cancel func of an in-flight request so the client
// can abort it via notifications/cancelled.
func (s *server) track(id json.RawMessage, cancel context.CancelFunc) {
	if len(id) == 0 {
		return
	}
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	s.inflight[string(id)] = cancel
}

func (s *server) untrack(id json.RawMessage) {
	if len(id) == 0 {
		return
	}
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	delete(s.inflight, string(id))
}

// cancelAll cancels every in-flight request — used for graceful shutdown
// when stdin reaches EOF: blocking tools observe ctx.Done, answer -32800
// and return (tearing down jobs whose ids never reached the client).
func (s *server) cancelAll() {
	s.reqMu.Lock()
	defer s.reqMu.Unlock()
	for _, cancel := range s.inflight {
		cancel()
	}
}

// handleCancelled implements notifications/cancelled: look up the
// referenced request and cancel its context. Notifications get no reply.
func (s *server) handleCancelled(req rpcRequest) {
	var p struct {
		RequestID json.RawMessage `json:"requestId"`
	}
	if err := json.Unmarshal(req.Params, &p); err != nil || len(p.RequestID) == 0 {
		return
	}
	s.reqMu.Lock()
	cancel := s.inflight[string(p.RequestID)]
	s.reqMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *server) handle(ctx context.Context, req rpcRequest) {
	isNotification := len(req.ID) == 0
	switch req.Method {
	case "initialize":
		s.sendResult(req.ID, map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]interface{}{"tools": map[string]interface{}{}},
			"serverInfo":      map[string]interface{}{"name": "mcp-exec", "version": "1.0.0"},
		})
	case "notifications/initialized", "initialized":
		// No response expected for notifications.
	case "notifications/cancelled", "cancelled":
		s.handleCancelled(req)
	case "ping":
		if !isNotification {
			s.sendResult(req.ID, map[string]interface{}{})
		}
	case "tools/list":
		s.sendResult(req.ID, map[string]interface{}{"tools": toolDefs()})
	case "tools/call":
		s.handleToolsCall(ctx, req)
	default:
		if !isNotification {
			s.sendError(req.ID, codeMethodNotFound, "method not found: "+req.Method)
		}
	}
}

func (s *server) handleToolsCall(ctx context.Context, req rpcRequest) {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
		Meta      struct {
			ProgressToken json.RawMessage `json:"progressToken"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		s.sendError(req.ID, codeInvalidParams, "invalid tools/call params: "+err.Error())
		return
	}
	if params.Arguments == nil {
		params.Arguments = json.RawMessage("{}")
	}
	rep := &progressReporter{srv: s, token: params.Meta.ProgressToken, every: s.mgr.cfg.progressInterval}
	var (
		result interface{}
		err    error
	)
	switch params.Name {
	case "execute":
		result, err = s.mgr.toolExecute(ctx, params.Arguments, rep)
	case "wait":
		result, err = s.mgr.toolWait(ctx, params.Arguments, rep)
	case "status":
		result, err = s.mgr.toolStatus(ctx, params.Arguments)
	case "output":
		result, err = s.mgr.toolOutput(ctx, params.Arguments)
	case "input":
		result, err = s.mgr.toolInput(ctx, params.Arguments)
	case "kill":
		result, err = s.mgr.toolKill(ctx, params.Arguments, rep)
	case "list_jobs":
		result, err = s.mgr.toolListJobs(ctx, params.Arguments)
	default:
		s.sendError(req.ID, codeMethodNotFound, "unknown tool: "+params.Name)
		return
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			s.sendError(req.ID, codeRequestCancelled, "request cancelled")
			return
		}
		s.sendResult(req.ID, map[string]interface{}{
			"content": []map[string]interface{}{
				{"type": "text", "text": err.Error()},
			},
			"isError": true,
		})
		return
	}
	payload, _ := json.Marshal(result)
	s.sendResult(req.ID, map[string]interface{}{
		"content": []map[string]interface{}{
			{"type": "text", "text": string(payload)},
		},
		"isError": false,
	})
}

// ---------------------------------------------------------------------
// main
// ---------------------------------------------------------------------

func main() {
	cfg := loadConfig()
	mgr := newManager(cfg)
	mgr.installOrphanReaper()
	srv := newServer(mgr, os.Stdout)
	reader := bufio.NewReaderSize(os.Stdin, 1<<20)
	var wg sync.WaitGroup
	for {
		line, err := readLine(reader)
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var req rpcRequest
		if jsonErr := json.Unmarshal([]byte(line), &req); jsonErr != nil {
			srv.sendError(nil, codeParseError, "parse error: "+jsonErr.Error())
			continue
		}
		if req.JSONRPC == "" {
			req.JSONRPC = "2.0"
		}
		// Handle each request concurrently so a blocking execute()/wait()
		// call doesn't stall other tool calls issued while it's
		// outstanding. Requests (with an id) get a cancellable context so
		// the client can abort them via notifications/cancelled.
		ctx, cancel := context.WithCancel(context.Background())
		srv.track(req.ID, cancel)
		wg.Add(1)
		go func(r rpcRequest, ctx context.Context, cancel context.CancelFunc) {
			defer wg.Done()
			defer cancel()
			defer srv.untrack(r.ID)
			srv.handle(ctx, r)
		}(req, ctx, cancel)
	}
	// Graceful shutdown: stdin reached EOF (the client went away). Cancel
	// every in-flight request — blocking tools answer -32800 and return,
	// tearing down any jobs whose ids never reached the client — then
	// wait for the handlers and (bounded) for the teardowns they spawned.
	srv.cancelAll()
	wg.Wait()
	mgr.awaitTeardowns(10 * time.Second)
}

// readLine reads a single newline-delimited message, growing beyond
// bufio.Scanner's fixed token limit so long commands/output/input payloads
// don't get rejected.
func readLine(r *bufio.Reader) (string, error) {
	var sb strings.Builder
	for {
		chunk, isPrefix, err := r.ReadLine()
		if len(chunk) > 0 {
			sb.Write(chunk)
		}
		if err != nil {
			if sb.Len() > 0 {
				return sb.String(), nil
			}
			return "", err
		}
		if !isPrefix {
			return sb.String(), nil
		}
	}
}
