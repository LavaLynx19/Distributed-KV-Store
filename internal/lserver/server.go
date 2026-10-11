// Package lserver is the real shell of the Leaderless variant (A§13.12): it
// gives a leaderless.Node a clock, a disk, TCP to the other Nodes and an
// HTTP API for clients. It is to internal/leaderless what internal/server
// is to the consensus core.
package lserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/leaderless"
	"distributed-kv-store/internal/transport"
)

// Config sets up a Node.
type Config struct {
	ID    int
	Peers map[int]string // every Node's address for other Nodes, this one's included
	// Listener accepts connections from other Nodes.
	Listener net.Listener
	// W, R, Mode and the switches are passed to the core
	// (leaderless.Config).
	W, R                                              int
	Mode                                              leaderless.Mode
	DeliverHints, Resend, Exchange, Repair, Forgetful bool
	// Data is the directory the Node keeps its state in. Empty keeps
	// nothing: a Node that restarts comes back with nothing.
	Data string
	// Tick is the length of one core tick. The core's waits are counted in
	// ticks, so this sets how long a write waits for a Home Node before a
	// stand-in is given it, and so on (leaderless: hintAfter and the rest).
	Tick time.Duration
}

// Server is one running Node.
type Server struct {
	cfg    Config
	events chan any
	tr     *transport.Transport
	log    *os.File
	refs   atomic.Uint64

	mu      sync.Mutex
	waiting map[uint64]chan leaderless.Reply
	// holds is what the Node held after the last step, for whoever looks
	// (State). The core itself is touched only by the loop.
	state atomic.Pointer[View]
}

// View is what a Node holds, as whoever checks a run sees it.
type View struct {
	// Keys is, per key the Node is a Home Node of or holds anything of,
	// its elements and its whole state as text.
	Keys  map[string]KeyView
	Hints int
}

// KeyView is one key in a View.
type KeyView struct {
	Values []string
	Canon  string
	Home   bool
}

type request struct{ req leaderless.Request }

// Open starts a Node. It replays what the Node had on disk first.
func Open(ctx context.Context, cfg Config) (*Server, error) {
	transport.Register(leaderless.MessageBodies()...)
	s := &Server{cfg: cfg, events: make(chan any, 4096), waiting: map[uint64]chan leaderless.Reply{}}
	var stored leaderless.Stored
	stored.Apply(nil)
	if cfg.Data != "" {
		if err := os.MkdirAll(cfg.Data, 0o755); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(filepath.Join(cfg.Data, "leaderless.log"), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, err
		}
		if err := replay(f, &stored); err != nil {
			f.Close()
			return nil, err
		}
		s.log = f
	}
	var ids []leaderless.NodeID
	peers := map[core.NodeID]string{}
	for id, addr := range cfg.Peers {
		ids = append(ids, leaderless.NodeID(id))
		peers[core.NodeID(id)] = addr
	}
	sortIDs(ids)
	node := leaderless.New(leaderless.Config{
		ID: leaderless.NodeID(cfg.ID), Nodes: ids, W: cfg.W, R: cfg.R, Mode: cfg.Mode,
		DeliverHints: cfg.DeliverHints, Resend: cfg.Resend, Exchange: cfg.Exchange, Repair: cfg.Repair, Forgetful: cfg.Forgetful,
		Stored: stored, Seed: uint64(time.Now().UnixNano()) >> 1,
	})
	s.tr = transport.New(core.NodeID(cfg.ID), cfg.Listener, peers, func(m core.Message) {
		select {
		case s.events <- leaderless.Receive{Msg: leaderless.Message{From: leaderless.NodeID(m.From), To: leaderless.NodeID(m.To), Body: m.Body}}:
		default: // the Node is swamped: a Message lost is one sent again
		}
	})
	s.look(node)
	go s.loop(ctx, node)
	return s, nil
}

func sortIDs(ids []leaderless.NodeID) {
	for i := range ids {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
}

// Close stops the Node's network and closes its file.
func (s *Server) Close() {
	s.tr.Close()
	if s.log != nil {
		s.log.Close()
	}
}

// loop is the only goroutine that touches the core. It takes whatever has
// arrived, steps the core through all of it, writes what that asks to be
// stored with one sync, and only then sends and answers.
func (s *Server) loop(ctx context.Context, node *leaderless.Node) {
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	for {
		var batch []leaderless.Output
		step := func(ev leaderless.Event) { batch = append(batch, node.Step(ev)) }
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			step(leaderless.Tick{})
		case ev := <-s.events:
			s.take(ev, step)
		}
		for more := true; more && len(batch) < 512; {
			select {
			case ev := <-s.events:
				s.take(ev, step)
			default:
				more = false
			}
		}
		var records []leaderless.Record
		for _, out := range batch {
			records = append(records, out.Persist...)
		}
		if err := s.persist(records); err != nil {
			// What couldn't be stored must not be acted on.
			fmt.Fprintf(os.Stderr, "lserver: node %d: storing: %v\n", s.cfg.ID, err)
			continue
		}
		for _, out := range batch {
			for _, m := range out.Messages {
				s.tr.Send(core.Message{From: core.NodeID(m.From), To: core.NodeID(m.To), Body: m.Body})
			}
			for _, r := range out.Replies {
				s.mu.Lock()
				back := s.waiting[r.Ref]
				delete(s.waiting, r.Ref)
				s.mu.Unlock()
				if back != nil {
					back <- r
				}
			}
		}
		s.look(node)
	}
}

func (s *Server) take(ev any, step func(leaderless.Event)) {
	switch ev := ev.(type) {
	case leaderless.Receive:
		step(ev)
	case request:
		step(ev.req)
	}
}

// look records what the Node holds, for State.
func (s *Server) look(node *leaderless.Node) {
	v := &View{Keys: map[string]KeyView{}, Hints: node.Hints()}
	for _, key := range node.Keys() {
		held := node.Holds(key)
		home := false
		for _, h := range node.Ring().Homes(key) {
			home = home || h == leaderless.NodeID(s.cfg.ID)
		}
		v.Keys[key] = KeyView{Values: held.Values(), Canon: held.Canon(), Home: home}
	}
	s.state.Store(v)
}

// State is what the Node held after its last step.
func (s *Server) State() View { return *s.state.Load() }

// ErrNoAnswer means the Node didn't answer in time. A write may have taken
// effect all the same.
var ErrNoAnswer = errors.New("lserver: no answer in time")

// Do carries out one request and waits for the Node's Reply.
func (s *Server) Do(ctx context.Context, op leaderless.Op, key, elem string, ttl time.Duration) (leaderless.Reply, error) {
	ref := s.refs.Add(1)
	back := make(chan leaderless.Reply, 1)
	s.mu.Lock()
	s.waiting[ref] = back
	s.mu.Unlock()
	forget := func() {
		s.mu.Lock()
		delete(s.waiting, ref)
		s.mu.Unlock()
	}
	req := request{leaderless.Request{Ref: ref, Op: op, Key: key, Elem: elem, Now: time.Now().UnixMilli(), TTL: ttl.Milliseconds()}}
	select {
	case s.events <- req:
	case <-ctx.Done():
		forget()
		return leaderless.Reply{}, ErrNoAnswer
	}
	select {
	case r := <-back:
		return r, nil
	case <-ctx.Done():
		forget()
		return leaderless.Reply{}, ErrNoAnswer
	}
}

// The Node's file is a run of frames: a length, a checksum, and that many
// bytes holding the Records of one batch. A frame that is short or doesn't
// match its checksum ends the file: it was being written when the Node
// stopped, and nothing in it was ever acted on.

func (s *Server) persist(records []leaderless.Record) error {
	if s.log == nil || len(records) == 0 {
		return nil
	}
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(records); err != nil {
		return err
	}
	var head [8]byte
	binary.BigEndian.PutUint32(head[:4], uint32(body.Len()))
	binary.BigEndian.PutUint32(head[4:], crc32.ChecksumIEEE(body.Bytes()))
	if _, err := s.log.Write(append(head[:], body.Bytes()...)); err != nil {
		return err
	}
	return s.log.Sync()
}

func replay(f *os.File, stored *leaderless.Stored) error {
	good := int64(0)
	for {
		var head [8]byte
		if _, err := io.ReadFull(f, head[:]); err != nil {
			break
		}
		body := make([]byte, binary.BigEndian.Uint32(head[:4]))
		if _, err := io.ReadFull(f, body); err != nil || crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(head[4:]) {
			break
		}
		var records []leaderless.Record
		if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&records); err != nil {
			break
		}
		stored.Apply(records)
		good += int64(len(head) + len(body))
	}
	if err := f.Truncate(good); err != nil {
		return err
	}
	_, err := f.Seek(good, io.SeekStart)
	return err
}
