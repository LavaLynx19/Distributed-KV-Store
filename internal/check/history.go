// Package check produces the three verdicts every run is judged by (A§8.2):
// whether the recorded History is Linearizable, whether every Member ends
// with identical data, and what the clients experienced.
package check

import (
	"fmt"
	"slices"
	"time"

	"github.com/anishathalye/porcupine"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/fsm"
)

// Result is one request's ending, as the client saw it.
type Result uint8

const (
	// Answered: the store returned a definite response.
	Answered Result = iota
	// Rejected: the store said it did not take the request (not the Leader,
	// no Majority, or the Member was down). It had no effect.
	Rejected
	// Lost: no definite answer. The request may or may not have taken effect.
	Lost
)

type call struct {
	client int
	cmd    fsm.Command
	start  int64
	end    int64
	result Result
	resp   fsm.Response
	done   bool
}

// History records every client request and how it ended, with times. It is
// not safe for concurrent use; real-run clients must serialize access.
type History struct {
	calls []call
	// Signals gathered while recording.
	Signals Signals
}

// Signals are the client-side view of a run (A§8.2, verdict 3).
type Signals struct {
	Answered int
	Rejected int
	Lost     int
	// okWrites are the times at which a put or delete was answered OK.
	okWrites []int64
}

// Begin records a request being sent at time now and returns its handle.
func (h *History) Begin(client int, cmd fsm.Command, now int64) int {
	h.calls = append(h.calls, call{client: client, cmd: cmd, start: now})
	return len(h.calls) - 1
}

// End records how request id finished at time now. resp matters only when
// the result is Answered.
func (h *History) End(id int, result Result, resp fsm.Response, now int64) {
	c := &h.calls[id]
	if c.done {
		panic(fmt.Sprintf("check: request %d ended twice", id))
	}
	c.done, c.end, c.result, c.resp = true, now, result, resp
	switch result {
	case Answered:
		h.Signals.Answered++
		if c.cmd.Op != fsm.OpGet && resp.Status == fsm.StatusOK {
			h.Signals.okWrites = append(h.Signals.okWrites, now)
		}
	case Rejected:
		h.Signals.Rejected++
	case Lost:
		h.Signals.Lost++
	}
}

// RecoveryAfter is how long after time t the next write was answered OK, or
// -1 if none was. With t set to the moment a Leader was lost, it is the
// recovery time the Rungs report.
func (s Signals) RecoveryAfter(t int64) int64 {
	for _, at := range s.okWrites {
		if at >= t {
			return at - t
		}
	}
	return -1
}

// Operations turns the History into the checker's input.
//   - A rejected request had no effect and is left out.
//   - A request still open, or lost, stays open to the end of the History: it
//     may take effect at any later moment, or never. A lost read has no
//     effect either way, so it is left out too.
func (h *History) Operations() []porcupine.Operation {
	var last int64
	for _, c := range h.calls {
		last = max(last, c.start, c.end)
	}
	var ops []porcupine.Operation
	for _, c := range h.calls {
		open := !c.done || c.result == Lost
		switch {
		case c.done && c.result == Rejected:
			continue
		case open && c.cmd.Op == fsm.OpGet:
			continue
		case open:
			ops = append(ops, porcupine.Operation{ClientId: c.client, Input: c.cmd, Call: c.start, Output: Outcome{Unknown: true}, Return: last + 1})
		default:
			ops = append(ops, porcupine.Operation{ClientId: c.client, Input: c.cmd, Call: c.start, Output: Outcome{Resp: c.resp}, Return: c.end})
		}
	}
	return ops
}

// Verdict is the linearizability verdict on a History.
type Verdict struct {
	// Linearizable is false only when the checker proved a violation.
	Linearizable bool
	// TimedOut: the checker ran out of time without finding a violation.
	TimedOut bool
	info     porcupine.LinearizationInfo
}

// Linearizable checks the History against the store's model, giving up
// after timeout (zero means no limit).
func (h *History) Linearizable(timeout time.Duration) Verdict {
	res, info := porcupine.CheckOperationsVerbose(Model(), h.Operations(), timeout)
	return Verdict{Linearizable: res != porcupine.Illegal, TimedOut: res == porcupine.Unknown, info: info}
}

// Visualize writes an HTML timeline of the checked History to path, showing
// where it stops being Linearizable.
func (v Verdict) Visualize(path string) error {
	return porcupine.VisualizePath(Model(), v.info, path)
}

// Diverged compares Members' final data (A§8.2, verdict 2). It returns one
// line per difference from the lowest-numbered Member, or nil if all agree.
func Diverged(items map[core.NodeID][]fsm.Item) []string {
	ids := make([]core.NodeID, 0, len(items))
	for id := range items {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if len(ids) < 2 {
		return nil
	}
	ref := index(items[ids[0]])
	var diffs []string
	for _, id := range ids[1:] {
		got := index(items[id])
		keys := map[string]bool{}
		for k := range ref {
			keys[k] = true
		}
		for k := range got {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		slices.Sort(sorted)
		for _, k := range sorted {
			a, inA := ref[k]
			b, inB := got[k]
			if inA != inB || string(a.Value) != string(b.Value) || a.Version != b.Version {
				diffs = append(diffs, fmt.Sprintf("key %q: node %d has %s, node %d has %s", k, ids[0], show(a, inA), id, show(b, inB)))
			}
		}
	}
	return diffs
}

func index(items []fsm.Item) map[string]fsm.Item {
	m := make(map[string]fsm.Item, len(items))
	for _, it := range items {
		m[it.Key] = it
	}
	return m
}

func show(it fsm.Item, exists bool) string {
	if !exists {
		return "nothing"
	}
	return fmt.Sprintf("%q v%d", it.Value, it.Version)
}
