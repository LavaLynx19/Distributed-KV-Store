// Package transport carries core Messages between Nodes over TCP (A§2.6).
// It promises nothing a consensus core doesn't already cope with: a Message
// may be lost when a connection breaks or a peer falls behind, and nothing is
// retried.
package transport

import (
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"net"
	"sync"
	"time"

	"distributed-kv-store/internal/core"
)

// Register tells the encoding which concrete types travel in Message.Body.
// Call it once per core, before any Message is sent or received.
func Register(bodies ...any) {
	for _, b := range bodies {
		gob.Register(b)
	}
}

// queueSize is how many Messages may wait for one peer before new ones are
// dropped.
const queueSize = 1024

// Transport is one Node's end of the network.
type Transport struct {
	id      core.NodeID
	ln      net.Listener
	deliver func(core.Message)
	// queues has one queue per peer, each emptied by its own sendLoop.
	// resolve, if set, finds the address of a peer there is no queue for
	// yet. qmu guards both.
	qmu     sync.RWMutex
	queues  map[core.NodeID]chan core.Message
	resolve func(core.NodeID) string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// New starts a Transport that accepts connections on ln and dials each peer
// in peers (id → address; the Node's own id is ignored). deliver is called
// for every Message received, from several goroutines.
func New(id core.NodeID, ln net.Listener, peers map[core.NodeID]string, deliver func(core.Message)) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &Transport{
		id: id, ln: ln, deliver: deliver,
		queues: map[core.NodeID]chan core.Message{},
		conns:  map[net.Conn]struct{}{},
		ctx:    ctx, cancel: cancel,
	}
	for peer, addr := range peers {
		if peer != id {
			t.connect(peer, addr)
		}
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t
}

// connect starts sending to a peer. The caller holds qmu, or is New.
func (t *Transport) connect(peer core.NodeID, addr string) chan core.Message {
	q := make(chan core.Message, queueSize)
	t.queues[peer] = q
	t.wg.Add(1)
	go t.sendLoop(addr, q)
	return q
}

// Resolve gives the Transport a way to find the address of a peer it wasn't
// told of at the start: one that joined the store later (A§11.10). fn
// returns "" for a peer it doesn't know either. It is called from Send.
func (t *Transport) Resolve(fn func(core.NodeID) string) {
	t.qmu.Lock()
	t.resolve = fn
	t.qmu.Unlock()
}

// Send queues a Message for its destination and returns at once. The Message
// is dropped if the peer is unknown or its queue is full.
func (t *Transport) Send(msg core.Message) {
	t.qmu.RLock()
	q, ok := t.queues[msg.To]
	resolve := t.resolve
	t.qmu.RUnlock()
	if !ok {
		if resolve == nil {
			return
		}
		addr := resolve(msg.To)
		if addr == "" {
			return
		}
		t.qmu.Lock()
		if q, ok = t.queues[msg.To]; !ok && t.ctx.Err() == nil {
			q, ok = t.connect(msg.To, addr), true
		}
		t.qmu.Unlock()
		if !ok {
			return
		}
	}
	select {
	case q <- msg:
	default:
	}
}

// Close stops every goroutine and closes every connection.
func (t *Transport) Close() {
	t.qmu.Lock() // no new sendLoop starts once the context is done
	t.cancel()
	t.qmu.Unlock()
	t.ln.Close()
	t.mu.Lock()
	for c := range t.conns {
		c.Close()
	}
	t.mu.Unlock()
	t.wg.Wait()
}

func (t *Transport) track(c net.Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ctx.Err() != nil {
		c.Close()
		return false
	}
	t.conns[c] = struct{}{}
	return true
}

func (t *Transport) untrack(c net.Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
	c.Close()
}

// sendLoop keeps one outgoing connection to a peer, redialing when it breaks.
// Messages that arrive while it is down are dropped rather than piled up.
func (t *Transport) sendLoop(addr string, q chan core.Message) {
	defer t.wg.Done()
	backoff := 20 * time.Millisecond
	for t.ctx.Err() == nil {
		var d net.Dialer
		conn, err := d.DialContext(t.ctx, "tcp", addr)
		if err != nil {
			drain(q)
			select {
			case <-t.ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, time.Second)
			continue
		}
		if !t.track(conn) {
			return
		}
		backoff = 20 * time.Millisecond
		t.write(conn, q)
		t.untrack(conn)
	}
}

func (t *Transport) write(conn net.Conn, q chan core.Message) {
	enc := gob.NewEncoder(conn)
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg := <-q:
			if err := enc.Encode(&msg); err != nil {
				return
			}
		}
	}
}

func drain(q chan core.Message) {
	for {
		select {
		case <-q:
		default:
			return
		}
	}
}

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			if t.ctx.Err() == nil {
				log.Printf("transport: node %d: accept: %v", t.id, err)
			}
			return
		}
		if !t.track(conn) {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.untrack(conn)
			dec := gob.NewDecoder(conn)
			for {
				var msg core.Message
				if err := dec.Decode(&msg); err != nil {
					return
				}
				t.deliver(msg)
			}
		}()
	}
}

// Loopback passes a Message through the same encoding the network uses and
// returns the copy that would arrive. The Simulation uses it so that a
// simulated Member can never share memory with another, and so that every
// simulated run also exercises the encoding. It is not safe for concurrent
// use.
type Loopback struct {
	buf bytes.Buffer
	enc *gob.Encoder
	dec *gob.Decoder
}

func NewLoopback() *Loopback {
	l := &Loopback{}
	l.enc = gob.NewEncoder(&l.buf)
	l.dec = gob.NewDecoder(&l.buf)
	return l
}

func (l *Loopback) Copy(msg core.Message) core.Message {
	if err := l.enc.Encode(&msg); err != nil {
		panic("transport: can't encode message: " + err.Error())
	}
	var out core.Message
	if err := l.dec.Decode(&out); err != nil {
		panic("transport: can't decode message: " + err.Error())
	}
	return out
}
