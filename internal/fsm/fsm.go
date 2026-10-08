// Package fsm is the state machine every Member applies Committed Entries to
// (A§5.2, A§5.3). It is pure: Members that apply the same Entries in the same
// order hold the same state and return the same responses.
package fsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"

	"distributed-kv-store/internal/core"
)

// Op is what a Command does.
type Op uint8

const (
	// OpGet reads a key. In Rung 1 a read goes through the Log (A§6.2).
	OpGet Op = iota + 1
	// OpPut sets a key.
	OpPut
	// OpDelete removes a key.
	OpDelete
	// OpOpenSession registers a Session. The response carries its id.
	OpOpenSession
)

// Command is one client request, carried in an Entry's payload.
type Command struct {
	Op    Op
	Key   string
	Value []byte
	// Conditional makes a put or delete a compare-and-set: it applies only if
	// the key's current version equals IfVersion. Version 0 means "the key
	// doesn't exist".
	Conditional bool
	IfVersion   uint64
	// Session and Seq identify the request, so that a retry takes effect
	// once (A§6.3). A client sends Seq 1, 2, 3… within its Session, and
	// repeats a Seq only to retry that request. Session 0 means no Session:
	// every copy of the request is applied.
	Session uint64
	Seq     uint64
}

// Status is the outcome of applying a Command.
type Status uint8

const (
	StatusOK Status = iota + 1
	// StatusNotFound: a get or an unconditional delete named a missing key.
	StatusNotFound
	// StatusVersionMismatch: a compare-and-set found a different version.
	StatusVersionMismatch
	// StatusInvalid: the Entry's payload wasn't a valid Command, or its Seq
	// is older than one the Session has already moved past.
	StatusInvalid
	// StatusSessionExpired: the Command named a Session the store doesn't
	// have.
	StatusSessionExpired
)

// Response is what the client gets back. Version is the key's version after
// the Command: the Index of the Entry that last wrote it, or 0 if it doesn't
// exist. On a mismatch it is the version found.
type Response struct {
	Status  Status
	Value   []byte
	Version uint64
	// Session is the new Session's id, in the answer to OpOpenSession.
	Session uint64
}

// Item is one key with its value and version.
type Item struct {
	Key     string
	Value   []byte
	Version uint64
}

type entry struct {
	value   []byte
	version uint64
}

// session is what the store remembers about one client: the last request it
// applied for that client, and what it answered.
type session struct {
	lastSeq  uint64
	lastResp []byte
}

// Machine holds the keys and the Sessions. The zero value is not usable;
// call New.
type Machine struct {
	keys     map[string]entry
	sessions map[uint64]*session
}

func New() *Machine {
	return &Machine{keys: map[string]entry{}, sessions: map[uint64]*session{}}
}

// Apply executes a Committed Entry and returns its encoded Response. A no-op
// Entry changes nothing and returns nil.
func (m *Machine) Apply(e core.Entry) []byte {
	if e.Kind != core.EntryCommand {
		return nil
	}
	cmd, err := DecodeCommand(e.Payload)
	if err != nil {
		return Response{Status: StatusInvalid}.Encode()
	}
	index := uint64(e.Index)
	if cmd.Op == OpOpenSession {
		// The Entry's Index is unique and the same on every Member, which
		// makes it a ready-made Session id.
		m.sessions[index] = &session{}
		return Response{Status: StatusOK, Session: index}.Encode()
	}
	if cmd.Session == 0 {
		return m.apply(cmd, index).Encode()
	}

	s, ok := m.sessions[cmd.Session]
	switch {
	case !ok:
		return Response{Status: StatusSessionExpired}.Encode()
	case cmd.Seq == s.lastSeq && s.lastSeq != 0:
		return s.lastResp // a retry: answer as before, change nothing
	case cmd.Seq < s.lastSeq:
		return Response{Status: StatusInvalid}.Encode()
	}
	s.lastSeq = cmd.Seq
	s.lastResp = m.apply(cmd, index).Encode()
	return s.lastResp
}

// Sessions is how many Sessions are open, for tests and End-state checks.
func (m *Machine) Sessions() int { return len(m.sessions) }

// Read answers an encoded get from the state as it stands, without an Entry.
// Whether that answer is safe to give a client is the core's call (A§6.2).
func (m *Machine) Read(query []byte) []byte {
	cmd, err := DecodeCommand(query)
	if err != nil || cmd.Op != OpGet {
		return Response{Status: StatusInvalid}.Encode()
	}
	return m.apply(cmd, 0).Encode()
}

// A key's version is the Index of the Entry that last wrote it. Versions
// therefore never repeat, even when a key is deleted and created again.
func (m *Machine) apply(cmd Command, index uint64) Response {
	cur, exists := m.keys[cmd.Key]
	switch cmd.Op {
	case OpGet:
		if !exists {
			return Response{Status: StatusNotFound}
		}
		return Response{Status: StatusOK, Value: cur.value, Version: cur.version}
	case OpPut:
		if cmd.Conditional && cur.version != cmd.IfVersion {
			return Response{Status: StatusVersionMismatch, Version: cur.version}
		}
		m.keys[cmd.Key] = entry{value: bytes.Clone(cmd.Value), version: index}
		return Response{Status: StatusOK, Version: index}
	case OpDelete:
		if cmd.Conditional && cur.version != cmd.IfVersion {
			return Response{Status: StatusVersionMismatch, Version: cur.version}
		}
		if !exists {
			return Response{Status: StatusNotFound}
		}
		delete(m.keys, cmd.Key)
		return Response{Status: StatusOK}
	}
	return Response{Status: StatusInvalid}
}

// Items lists every key in key order, for End-state comparison (A§8.2).
func (m *Machine) Items() []Item {
	items := make([]Item, 0, len(m.keys))
	for k, e := range m.keys {
		items = append(items, Item{Key: k, Value: e.value, Version: e.version})
	}
	slices.SortFunc(items, func(a, b Item) int { return cmpString(a.Key, b.Key) })
	return items
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

var errMalformed = errors.New("fsm: malformed payload")

// Encode lays a Command out as: op, flags, if-version, session, seq, key
// length, key, value length, value. Integers are unsigned varints.
func (c Command) Encode() []byte {
	b := make([]byte, 0, 4+len(c.Key)+len(c.Value)+5*binary.MaxVarintLen64)
	b = append(b, byte(c.Op))
	var flags byte
	if c.Conditional {
		flags = 1
	}
	b = append(b, flags)
	b = binary.AppendUvarint(b, c.IfVersion)
	b = binary.AppendUvarint(b, c.Session)
	b = binary.AppendUvarint(b, c.Seq)
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	b = binary.AppendUvarint(b, uint64(len(c.Value)))
	b = append(b, c.Value...)
	return b
}

func DecodeCommand(b []byte) (Command, error) {
	if len(b) < 2 {
		return Command{}, errMalformed
	}
	c := Command{Op: Op(b[0]), Conditional: b[1]&1 != 0}
	if c.Op < OpGet || c.Op > OpOpenSession {
		return Command{}, errMalformed
	}
	b = b[2:]
	var ok bool
	if c.IfVersion, b, ok = uvarint(b); !ok {
		return Command{}, errMalformed
	}
	if c.Session, b, ok = uvarint(b); !ok {
		return Command{}, errMalformed
	}
	if c.Seq, b, ok = uvarint(b); !ok {
		return Command{}, errMalformed
	}
	var key []byte
	if key, b, ok = lengthPrefixed(b); !ok {
		return Command{}, errMalformed
	}
	c.Key = string(key)
	if c.Value, b, ok = lengthPrefixed(b); !ok || len(b) != 0 {
		return Command{}, errMalformed
	}
	return c, nil
}

// Encode lays a Response out as: status, version, session, value length,
// value.
func (r Response) Encode() []byte {
	b := make([]byte, 0, 1+len(r.Value)+3*binary.MaxVarintLen64)
	b = append(b, byte(r.Status))
	b = binary.AppendUvarint(b, r.Version)
	b = binary.AppendUvarint(b, r.Session)
	b = binary.AppendUvarint(b, uint64(len(r.Value)))
	b = append(b, r.Value...)
	return b
}

func DecodeResponse(b []byte) (Response, error) {
	if len(b) < 1 {
		return Response{}, errMalformed
	}
	r := Response{Status: Status(b[0])}
	b = b[1:]
	var ok bool
	if r.Version, b, ok = uvarint(b); !ok {
		return Response{}, errMalformed
	}
	if r.Session, b, ok = uvarint(b); !ok {
		return Response{}, errMalformed
	}
	if r.Value, b, ok = lengthPrefixed(b); !ok || len(b) != 0 {
		return Response{}, errMalformed
	}
	return r, nil
}

func uvarint(b []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, nil, false
	}
	return v, b[n:], true
}

// lengthPrefixed reads a varint length and that many bytes. An empty field
// decodes as nil.
func lengthPrefixed(b []byte) (field, rest []byte, ok bool) {
	n, b, ok := uvarint(b)
	if !ok || n > uint64(len(b)) {
		return nil, nil, false
	}
	if n == 0 {
		return nil, b, true
	}
	return bytes.Clone(b[:n]), b[n:], true
}
