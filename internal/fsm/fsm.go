// Package fsm is the state machine every Member applies Committed Entries to
// (A§5.2, A§5.3). It is pure: Members that apply the same Entries in the same
// order hold the same state and return the same responses.
package fsm

import (
	"bytes"
	"encoding/binary"
	"errors"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/tree"
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
	// OpTick changes nothing itself. It carries a Stamp, so that time moves
	// in a Group nobody is writing to (A§6.7). A Leader's shell proposes one
	// when Due says so.
	OpTick
	// OpScan reads the keys from Key up to but not including End, in key
	// order, Limit at most. An empty End means to the last key (A§7.1).
	OpScan
	// OpTxn is a Transaction: if every key in Conds has the version given,
	// all of Writes are applied, and otherwise none are (A§5.2).
	OpTxn

	lastOp = OpTxn
)

// MaxScan is the most keys one scan returns, whatever Limit it asks for.
const MaxScan = 1000

// Cond is one condition of a Transaction: Key's version must be Version.
// Version 0 means the key must not exist.
type Cond struct {
	Key     string
	Version uint64
}

// Write is one change a Transaction makes. Op is OpPut or OpDelete. A
// delete of a key that doesn't exist changes nothing and is not a failure.
type Write struct {
	Op    Op
	Key   string
	Value []byte
	TTL   int64
}

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
	// Stamp is the Leader's clock reading when it took the request (A§6.1).
	// The shell on the Member that receives a request sets it; only a
	// Leader's copy ever reaches the Log. Zero means no reading.
	Stamp int64
	// TTL, on a put, is how long the key lives, in the units of Stamp. Zero
	// means for good.
	TTL int64
	// End and Limit bound an OpScan. Conds and Writes make up an OpTxn.
	End    string
	Limit  uint64
	Conds  []Cond
	Writes []Write
}

// Stamp returns the encoded Command with its Stamp set to now. A shell calls
// it on a request it is about to propose.
func Stamp(payload []byte, now int64) []byte {
	cmd, err := DecodeCommand(payload)
	if err != nil {
		return payload // Apply will answer that it is invalid
	}
	cmd.Stamp = now
	return cmd.Encode()
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
// exist. On a mismatch it is the version found. For a Transaction that was
// applied it is the version every key it put now has.
type Response struct {
	Status  Status
	Value   []byte
	Version uint64
	// Session is the new Session's id, in the answer to OpOpenSession.
	Session uint64
	// Items are the keys a scan found. For a Transaction that was refused
	// they are the conditions that failed, each with the version found.
	Items []Item
}

// Item is one key with its value and version. Expires is its deadline, or
// zero if it has none.
type Item struct {
	Key     string
	Value   []byte
	Version uint64
	Expires int64 `json:",omitempty"`
}

// entry is what a key holds. A deadline of zero means it never expires.
type entry struct {
	value    []byte
	version  uint64
	deadline int64
}

// session is what the store remembers about one client: the last request it
// applied for that client, and what it answered.
type session struct {
	lastSeq  uint64
	lastResp []byte
	// lastUsed is Log time when the Session was opened or last used.
	lastUsed int64
}

// Machine holds the keys and the Sessions, each in a copy-on-write tree
// (A§5.3), so the whole state can be captured at any moment by keeping the
// two roots. Sessions are keyed by their id as 8 big-endian bytes, which
// sorts them numerically.
//
// Time (A§6.7): logTime is the highest Stamp applied so far, and a key with
// a deadline stops existing once logTime reaches it. Every Member applies
// the same Stamps in the same order, so every Member agrees on which keys
// exist after any given Entry. expiries lists the keys that have a deadline,
// in deadline order, so the ones that are due are found without a search.
type Machine struct {
	keys     tree.Tree[entry]
	sessions tree.Tree[session]
	expiries tree.Tree[struct{}]
	logTime  int64

	// SessionTTL, if set, removes a Session that hasn't been used for this
	// long by Log time. A request in a removed Session is answered
	// StatusSessionExpired (A§6.3). Every Member must use the same value.
	SessionTTL int64

	// ownClock is the store Rung 5 starts from: time is whatever this
	// Member's clock says, given to Observe.
	ownClock bool
}

// advance moves Log time to a later Stamp and removes what that makes due.
// Sessions are checked each time Log time enters a new quarter of
// SessionTTL, so the work is spread out and still happens at the same Entry
// on every Member.
func (m *Machine) advance(to int64) {
	from := m.logTime
	m.logTime = to
	m.sweep()
	if m.SessionTTL <= 0 {
		return
	}
	if q := max(m.SessionTTL/4, 1); floorDiv(from, q) == floorDiv(to, q) {
		return
	}
	var idle []string
	m.sessions.Ascend("", "", func(k string, s session) bool {
		if to-s.lastUsed >= m.SessionTTL {
			idle = append(idle, k)
		}
		return true
	})
	for _, k := range idle {
		m.sessions = m.sessions.Delete(k)
	}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

func New() *Machine { return &Machine{} }

// NewOwnClock returns a Machine that judges Expiry by its Member's own
// clock: a deadline is the Leader's reading plus the time-to-live, and the
// key is gone once the clock handed to Observe reaches it. Members whose
// clocks differ then disagree about which keys exist. It exists so that
// Rung 5's exposure of that stays reproducible.
func NewOwnClock() *Machine { return &Machine{ownClock: true} }

// Observe tells the Machine what its Member's clock reads. A shell calls it
// before Apply and Read. Only a Machine from NewOwnClock takes any notice.
func (m *Machine) Observe(now int64) {
	if m.ownClock {
		m.logTime = now
		m.sweep()
	}
}

// LogTime is the time Expiry is judged against.
func (m *Machine) LogTime() int64 { return m.logTime }

// NextDeadline is the earliest deadline any key has, and whether there is
// one.
func (m *Machine) NextDeadline() (int64, bool) {
	var first int64
	found := false
	m.expiries.Ascend("", "", func(k string, _ struct{}) bool {
		first, found = expiryDeadline(k), true
		return false
	})
	return first, found
}

// Due reports whether a Leader whose clock reads now should propose an
// OpTick: some key's deadline has passed by that clock, and applying the
// Stamp would move logTime far enough to remove it.
func (m *Machine) Due(now int64) bool {
	deadline, ok := m.NextDeadline()
	return ok && !m.ownClock && deadline <= now && now > m.logTime
}

// expiryKey orders keys by deadline, then by name. The sign bit is flipped
// so that negative deadlines sort before positive ones.
func expiryKey(deadline int64, key string) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(deadline)^(1<<63))
	return string(b[:]) + key
}

func expiryDeadline(k string) int64 {
	return int64(binary.BigEndian.Uint64([]byte(k[:8])) ^ (1 << 63))
}

// sweep removes every key whose deadline logTime has reached. It runs
// whenever logTime moves, so no expired key is ever left to be read.
func (m *Machine) sweep() {
	for {
		var due string
		m.expiries.Ascend("", "", func(k string, _ struct{}) bool {
			if expiryDeadline(k) <= m.logTime {
				due = k
			}
			return false
		})
		if due == "" {
			return
		}
		m.expiries = m.expiries.Delete(due)
		m.keys = m.keys.Delete(due[8:])
	}
}

// set writes a key, keeping expiries in step.
func (m *Machine) set(key string, e entry) {
	m.unset(key)
	m.keys = m.keys.Put(key, e)
	if e.deadline != 0 {
		m.expiries = m.expiries.Put(expiryKey(e.deadline, key), struct{}{})
	}
}

// unset removes a key, keeping expiries in step.
func (m *Machine) unset(key string) {
	if old, ok := m.keys.Get(key); ok {
		if old.deadline != 0 {
			m.expiries = m.expiries.Delete(expiryKey(old.deadline, key))
		}
		m.keys = m.keys.Delete(key)
	}
}

func sessionKey(id uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return string(b[:])
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
	if !m.ownClock && cmd.Stamp > m.logTime {
		m.advance(cmd.Stamp)
	}
	index := uint64(e.Index)
	if cmd.Op == OpOpenSession {
		// The Entry's Index is unique and the same on every Member, which
		// makes it a ready-made Session id.
		m.sessions = m.sessions.Put(sessionKey(index), session{lastUsed: m.logTime})
		return Response{Status: StatusOK, Session: index}.Encode()
	}
	if cmd.Session == 0 {
		return m.apply(cmd, index).Encode()
	}

	s, ok := m.sessions.Get(sessionKey(cmd.Session))
	switch {
	case !ok:
		return Response{Status: StatusSessionExpired}.Encode()
	case cmd.Seq == s.lastSeq && s.lastSeq != 0:
		if s.lastUsed != m.logTime {
			s.lastUsed = m.logTime
			m.sessions = m.sessions.Put(sessionKey(cmd.Session), s)
		}
		return s.lastResp // a retry: answer as before, change nothing
	case cmd.Seq < s.lastSeq:
		return Response{Status: StatusInvalid}.Encode()
	}
	resp := m.apply(cmd, index).Encode()
	m.sessions = m.sessions.Put(sessionKey(cmd.Session), session{lastSeq: cmd.Seq, lastResp: resp, lastUsed: m.logTime})
	return resp
}

// Sessions is how many Sessions are open, for tests and End-state checks.
func (m *Machine) Sessions() int { return m.sessions.Len() }

// Read answers an encoded get from the state as it stands, without an Entry.
// Whether that answer is safe to give a client is the core's call (A§6.2).
func (m *Machine) Read(query []byte) []byte {
	cmd, err := DecodeCommand(query)
	if err != nil || (cmd.Op != OpGet && cmd.Op != OpScan) {
		return Response{Status: StatusInvalid}.Encode()
	}
	return m.apply(cmd, 0).Encode()
}

// A key's version is the Index of the Entry that last wrote it. Versions
// therefore never repeat, even when a key is deleted and created again.
func (m *Machine) apply(cmd Command, index uint64) Response {
	cur, exists := m.keys.Get(cmd.Key)
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
		m.set(cmd.Key, entry{value: bytes.Clone(cmd.Value), version: index, deadline: m.deadline(cmd.Stamp, cmd.TTL)})
		return Response{Status: StatusOK, Version: index}
	case OpDelete:
		if cmd.Conditional && cur.version != cmd.IfVersion {
			return Response{Status: StatusVersionMismatch, Version: cur.version}
		}
		if !exists {
			return Response{Status: StatusNotFound}
		}
		m.unset(cmd.Key)
		return Response{Status: StatusOK}
	case OpTick:
		return Response{Status: StatusOK}
	case OpScan:
		limit := cmd.Limit
		if limit == 0 || limit > MaxScan {
			limit = MaxScan
		}
		resp := Response{Status: StatusOK}
		m.keys.Ascend(cmd.Key, cmd.End, func(k string, e entry) bool {
			resp.Items = append(resp.Items, Item{Key: k, Value: e.value, Version: e.version})
			return uint64(len(resp.Items)) < limit
		})
		return resp
	case OpTxn:
		var failed []Item
		for _, c := range cmd.Conds {
			if found, _ := m.keys.Get(c.Key); found.version != c.Version {
				failed = append(failed, Item{Key: c.Key, Version: found.version})
			}
		}
		if len(failed) > 0 {
			return Response{Status: StatusVersionMismatch, Items: failed}
		}
		for _, w := range cmd.Writes {
			switch w.Op {
			case OpPut:
				m.set(w.Key, entry{value: bytes.Clone(w.Value), version: index, deadline: m.deadline(cmd.Stamp, w.TTL)})
			case OpDelete:
				m.unset(w.Key)
			default:
				// DecodeCommand lets nothing else through.
			}
		}
		return Response{Status: StatusOK, Version: index}
	}
	return Response{Status: StatusInvalid}
}

// deadline is when a key written now with this time-to-live stops existing,
// or zero if it has none. It counts from logTime, which every Member agrees
// on. The naive store counts from the Leader's reading and leaves each
// Member to compare it with its own clock.
func (m *Machine) deadline(stamp, ttl int64) int64 {
	if ttl <= 0 {
		return 0
	}
	d := m.logTime + ttl
	if m.ownClock {
		d = stamp + ttl
	}
	if d == 0 {
		d = 1 // zero means no deadline
	}
	return d
}

// Items lists every key in key order, for End-state comparison (A§8.2).
func (m *Machine) Items() []Item {
	items := make([]Item, 0, m.keys.Len())
	m.keys.Ascend("", "", func(k string, e entry) bool {
		items = append(items, Item{Key: k, Value: e.value, Version: e.version, Expires: e.deadline})
		return true
	})
	return items
}

var errMalformed = errors.New("fsm: malformed payload")

// Encode lays a Command out as: op, flags, if-version, session, seq, key
// length, key, value length, value. Integers are unsigned varints. A Command
// with a Stamp or a TTL sets the timed flag and follows with those two, as
// signed varints. A scan or a Transaction sets the extended flag and ends
// with its own fields.
func (c Command) Encode() []byte {
	b := make([]byte, 0, 4+len(c.Key)+len(c.Value)+7*binary.MaxVarintLen64)
	b = append(b, byte(c.Op))
	var flags byte
	if c.Conditional {
		flags = flagConditional
	}
	timed := c.Stamp != 0 || c.TTL != 0
	if timed {
		flags |= flagTimed
	}
	extended := c.Op == OpScan || c.Op == OpTxn
	if extended {
		flags |= flagExtended
	}
	b = append(b, flags)
	b = binary.AppendUvarint(b, c.IfVersion)
	b = binary.AppendUvarint(b, c.Session)
	b = binary.AppendUvarint(b, c.Seq)
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	b = binary.AppendUvarint(b, uint64(len(c.Value)))
	b = append(b, c.Value...)
	if timed {
		b = binary.AppendVarint(b, c.Stamp)
		b = binary.AppendVarint(b, c.TTL)
	}
	switch c.Op {
	case OpScan:
		b = appendBytes(b, []byte(c.End))
		b = binary.AppendUvarint(b, c.Limit)
	case OpTxn:
		b = binary.AppendUvarint(b, uint64(len(c.Conds)))
		for _, cond := range c.Conds {
			b = appendBytes(b, []byte(cond.Key))
			b = binary.AppendUvarint(b, cond.Version)
		}
		b = binary.AppendUvarint(b, uint64(len(c.Writes)))
		for _, w := range c.Writes {
			b = append(b, byte(w.Op))
			b = appendBytes(b, []byte(w.Key))
			b = appendBytes(b, w.Value)
			b = binary.AppendVarint(b, w.TTL)
		}
	}
	return b
}

const (
	flagConditional = 1 << iota
	flagTimed
	flagExtended
)

func DecodeCommand(b []byte) (Command, error) {
	if len(b) < 2 {
		return Command{}, errMalformed
	}
	c := Command{Op: Op(b[0]), Conditional: b[1]&flagConditional != 0}
	timed := b[1]&flagTimed != 0
	if extended := b[1]&flagExtended != 0; extended != (c.Op == OpScan || c.Op == OpTxn) {
		return Command{}, errMalformed
	}
	if c.Op < OpGet || c.Op > lastOp {
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
	if c.Value, b, ok = lengthPrefixed(b); !ok {
		return Command{}, errMalformed
	}
	if timed {
		if c.Stamp, b, ok = varint(b); !ok {
			return Command{}, errMalformed
		}
		if c.TTL, b, ok = varint(b); !ok || c.TTL < 0 {
			return Command{}, errMalformed
		}
	}
	switch c.Op {
	case OpScan:
		var end []byte
		if end, b, ok = lengthPrefixed(b); !ok {
			return Command{}, errMalformed
		}
		c.End = string(end)
		if c.Limit, b, ok = uvarint(b); !ok {
			return Command{}, errMalformed
		}
	case OpTxn:
		var n uint64
		if n, b, ok = uvarint(b); !ok || n > uint64(len(b)) {
			return Command{}, errMalformed
		}
		for i := uint64(0); i < n; i++ {
			var key []byte
			var cond Cond
			if key, b, ok = lengthPrefixed(b); !ok {
				return Command{}, errMalformed
			}
			cond.Key = string(key)
			if cond.Version, b, ok = uvarint(b); !ok {
				return Command{}, errMalformed
			}
			c.Conds = append(c.Conds, cond)
		}
		if n, b, ok = uvarint(b); !ok || n > uint64(len(b)) {
			return Command{}, errMalformed
		}
		for i := uint64(0); i < n; i++ {
			if len(b) == 0 {
				return Command{}, errMalformed
			}
			w := Write{Op: Op(b[0])}
			var key []byte
			if key, b, ok = lengthPrefixed(b[1:]); !ok || (w.Op != OpPut && w.Op != OpDelete) {
				return Command{}, errMalformed
			}
			w.Key = string(key)
			if w.Value, b, ok = lengthPrefixed(b); !ok {
				return Command{}, errMalformed
			}
			if w.TTL, b, ok = varint(b); !ok || w.TTL < 0 {
				return Command{}, errMalformed
			}
			c.Writes = append(c.Writes, w)
		}
	}
	if len(b) != 0 {
		return Command{}, errMalformed
	}
	return c, nil
}

// Encode lays a Response out as: status, version, session, value length,
// value. A Response with Items follows that with their count and, for each,
// its key, value and version.
func (r Response) Encode() []byte {
	b := make([]byte, 0, 1+len(r.Value)+3*binary.MaxVarintLen64)
	b = append(b, byte(r.Status))
	b = binary.AppendUvarint(b, r.Version)
	b = binary.AppendUvarint(b, r.Session)
	b = binary.AppendUvarint(b, uint64(len(r.Value)))
	b = append(b, r.Value...)
	if len(r.Items) == 0 {
		return b
	}
	b = binary.AppendUvarint(b, uint64(len(r.Items)))
	for _, it := range r.Items {
		b = appendBytes(b, []byte(it.Key))
		b = appendBytes(b, it.Value)
		b = binary.AppendUvarint(b, it.Version)
	}
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
	if r.Value, b, ok = lengthPrefixed(b); !ok {
		return Response{}, errMalformed
	}
	if len(b) == 0 {
		return r, nil
	}
	var n uint64
	if n, b, ok = uvarint(b); !ok || n == 0 || n > uint64(len(b)) {
		return Response{}, errMalformed
	}
	for i := uint64(0); i < n; i++ {
		var key []byte
		var it Item
		if key, b, ok = lengthPrefixed(b); !ok {
			return Response{}, errMalformed
		}
		it.Key = string(key)
		if it.Value, b, ok = lengthPrefixed(b); !ok {
			return Response{}, errMalformed
		}
		if it.Version, b, ok = uvarint(b); !ok {
			return Response{}, errMalformed
		}
		r.Items = append(r.Items, it)
	}
	if len(b) != 0 {
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

func varint(b []byte) (int64, []byte, bool) {
	v, n := binary.Varint(b)
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

// Capture returns a function that encodes the state as it is right now. The
// function can be called later, or from another goroutine, while the Machine
// goes on applying Entries: it holds the two tree roots, which never change
// (A§6.4).
//
// The encoding is the keys, then the Sessions, then, only if the Machine has
// seen any time at all, a section with logTime, every key's deadline, and
// when each Session was last used.
func (m *Machine) Capture() func() []byte {
	keys, sessions, expiries, logTime := m.keys, m.sessions, m.expiries, m.logTime
	return func() []byte {
		b := binary.AppendUvarint(nil, uint64(keys.Len()))
		keys.Ascend("", "", func(k string, e entry) bool {
			b = appendBytes(b, []byte(k))
			b = binary.AppendUvarint(b, e.version)
			b = appendBytes(b, e.value)
			return true
		})
		b = binary.AppendUvarint(b, uint64(sessions.Len()))
		sessions.Ascend("", "", func(k string, s session) bool {
			b = append(b, k...) // always 8 bytes
			b = binary.AppendUvarint(b, s.lastSeq)
			b = appendBytes(b, s.lastResp)
			return true
		})
		if logTime == 0 && expiries.Len() == 0 {
			return b
		}
		b = binary.AppendVarint(b, logTime)
		b = binary.AppendUvarint(b, uint64(expiries.Len()))
		expiries.Ascend("", "", func(k string, _ struct{}) bool {
			b = binary.AppendVarint(b, expiryDeadline(k))
			b = appendBytes(b, []byte(k[8:]))
			return true
		})
		sessions.Ascend("", "", func(_ string, s session) bool {
			b = binary.AppendVarint(b, s.lastUsed)
			return true
		})
		return b
	}
}

// Restore replaces the Machine's state with a captured one.
func (m *Machine) Restore(data []byte) error {
	var keys tree.Tree[entry]
	var sessions tree.Tree[session]
	var sessionIDs []string // in the order they were encoded
	n, b, ok := uvarint(data)
	for i := uint64(0); ok && i < n; i++ {
		var k, v []byte
		var version uint64
		if k, b, ok = lengthPrefixed(b); !ok {
			break
		}
		if version, b, ok = uvarint(b); !ok {
			break
		}
		if v, b, ok = lengthPrefixed(b); ok {
			keys = keys.Put(string(k), entry{value: v, version: version})
		}
	}
	if ok {
		n, b, ok = uvarint(b)
	}
	for i := uint64(0); ok && i < n; i++ {
		if len(b) < 8 {
			ok = false
			break
		}
		id := string(b[:8])
		var s session
		if s.lastSeq, b, ok = uvarint(b[8:]); !ok {
			break
		}
		if s.lastResp, b, ok = lengthPrefixed(b); ok {
			sessions = sessions.Put(id, s)
			sessionIDs = append(sessionIDs, id)
		}
	}
	var expiries tree.Tree[struct{}]
	var logTime int64
	if ok && len(b) != 0 {
		if logTime, b, ok = varint(b); ok {
			n, b, ok = uvarint(b)
		}
		for i := uint64(0); ok && i < n; i++ {
			var deadline int64
			var k []byte
			if deadline, b, ok = varint(b); !ok {
				break
			}
			if k, b, ok = lengthPrefixed(b); !ok {
				break
			}
			e, found := keys.Get(string(k))
			if !found || deadline == 0 {
				ok = false
				break
			}
			e.deadline = deadline
			keys = keys.Put(string(k), e)
			expiries = expiries.Put(expiryKey(deadline, string(k)), struct{}{})
		}
		for _, id := range sessionIDs {
			if !ok {
				break
			}
			s, _ := sessions.Get(id)
			if s.lastUsed, b, ok = varint(b); ok {
				sessions = sessions.Put(id, s)
			}
		}
	}
	if !ok || len(b) != 0 {
		return errMalformed
	}
	m.keys, m.sessions, m.expiries, m.logTime = keys, sessions, expiries, logTime
	return nil
}

func appendBytes(b, field []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(field)))
	return append(b, field...)
}
