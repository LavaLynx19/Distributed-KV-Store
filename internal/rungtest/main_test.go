package rungtest_test

import (
	"os"
	"testing"

	"distributed-kv-store/internal/naive"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/transport"
)

func TestMain(m *testing.M) {
	transport.Register(naive.MessageBodies()...)
	transport.Register(raft.MessageBodies()...)
	os.Exit(m.Run())
}
