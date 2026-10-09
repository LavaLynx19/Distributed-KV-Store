package server

import (
	"time"

	"distributed-kv-store/internal/fsm"
)

// TimeEntries is a Node.TimeEntry for the key-value state machine: a Leader
// proposes an OpTick whenever a key's deadline has passed by its own clock,
// so the key goes even if nobody writes anything (A§6.7). now is the
// Member's clock in milliseconds; nil means the system clock.
func TimeEntries(now func() int64) func(Machine) []byte {
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return func(m Machine) []byte {
		t := now()
		if machine, ok := m.(*fsm.Machine); !ok || !machine.Due(t) {
			return nil
		}
		return fsm.Command{Op: fsm.OpTick, Stamp: t}.Encode()
	}
}
