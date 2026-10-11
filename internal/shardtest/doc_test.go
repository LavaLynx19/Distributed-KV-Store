// Package shardtest holds the Rung tests for a store with several Groups
// (Rungs 7 and 8). They run rungtest.RunSharded, and are apart from the
// tests of Rungs 1–6 only so that neither package's tests run for longer
// than Go allows one package.
package shardtest

import (
	"os"
	"testing"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/transport"
)

func raftConfig(id core.NodeID, members []core.NodeID, rng core.Rand, reads raft.ReadMode) raft.Config {
	return raft.Config{ID: id, Members: members, ElectionTicks: 10, HeartbeatTicks: 1, Rand: rng, Reads: reads}
}

func TestMain(m *testing.M) {
	transport.Register(raft.MessageBodies()...)
	os.Exit(m.Run())
}
