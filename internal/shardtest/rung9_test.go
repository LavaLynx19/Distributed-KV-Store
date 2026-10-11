package shardtest

import (
	"os"
	"testing"

	"distributed-kv-store/internal/leaderless"
	"distributed-kv-store/internal/lsim"
	"distributed-kv-store/internal/rungtest"
)

// consistent is the store the Leaderless variant is compared with: Rung 8's,
// five Nodes and three Groups, each key on three Nodes, with clients that
// send single-key requests and each talk to one Node.
var consistent = sharded(func(s *rungtest.ShardedStore) { s.Transactions.On = true })

func leaderlessStore(w, r int) lsim.Store {
	return lsim.Store{Nodes: 5, W: w, R: r, Mode: leaderless.AddWins, DeliverHints: true, Resend: true, Workload: lsim.DefaultWorkload}
}

// What follows measures how much of a store's work goes on through a
// Partition: the requests answered that began while two Nodes were cut off
// from three, as a share of those answered over the same time, from the
// same seeds, with nothing wrong. Counting the share of requests answered
// would flatter a store whose clients wait: a client that is kept waiting
// asks for little, and so fails little.

func consistentWork(sc rungtest.ShardedScenario, seeds uint64) (answered, failed int) {
	from, to := rungtest.FaultWindow()
	for seed := uint64(1); seed <= seeds; seed++ {
		r := rungtest.RunSharded(consistent, sc, seed)
		if !r.Passed() {
			failed++
		}
		a, _ := r.History.Share(from, to)
		answered += a
	}
	return answered, failed
}

func leaderlessWork(w, r int, sc lsim.Scenario, seeds uint64) (answered, lost int) {
	for seed := uint64(1); seed <= seeds; seed++ {
		rep := lsim.Run(leaderlessStore(w, r), sc, seed)
		answered += rep.WindowAnswered
		if len(rep.Lost) > 0 || len(rep.Back) > 0 {
			lost++
		}
	}
	return answered, lost
}

func lsimScenario(t *testing.T, name string) lsim.Scenario {
	t.Helper()
	for _, sc := range lsim.Scenarios {
		if sc.Name == name {
			return sc
		}
	}
	t.Fatalf("no scenario %q", name)
	return lsim.Scenario{}
}

// What Rung 9 is for: with two Nodes cut off from three, the consistent
// store stays correct and its clients on the small side get nothing done.
// The Leaderless variant at W=1 R=1 carries on nearly as if nothing had
// happened, and with sets where add wins loses nothing for it.
func TestTheLeaderlessVariantWorksThroughAPartition(t *testing.T) {
	cut, failed := consistentWork(rungtest.LongPartition, 20)
	whole, _ := consistentWork(shardedScenario(t, "no-moves"), 20)
	lcut, lost := leaderlessWork(1, 1, lsimScenario(t, "long-partition"), 20)
	lwhole, _ := leaderlessWork(1, 1, lsimScenario(t, "no-faults"), 20)
	if failed > 0 || lost > 0 {
		t.Fatalf("%d runs of the consistent store failed a verdict; %d of the Leaderless variant lost an add", failed, lost)
	}
	consistentShare, leaderlessShare := float64(cut)/float64(whole), float64(lcut)/float64(lwhole)
	if consistentShare > 0.75 || leaderlessShare < 0.9 {
		t.Errorf("through the Partition the consistent store did %.0f%% of its work and the Leaderless variant %.0f%%", 100*consistentShare, 100*leaderlessShare)
	}
}

// TestMeasureRung9Availability prints the comparison for the retro. It runs
// only when asked for: KV_MEASURE=1.
func TestMeasureRung9Availability(t *testing.T) {
	if os.Getenv("KV_MEASURE") == "" {
		t.Skip("set KV_MEASURE=1")
	}
	const seeds = 300
	cut, failed := consistentWork(rungtest.LongPartition, seeds)
	whole, _ := consistentWork(shardedScenario(t, "no-moves"), seeds)
	t.Logf("consistent store (Rung 8)   answered %6d requests through the Partition and %6d with none: %5.1f%%; %d of %d runs failed a verdict", cut, whole, 100*float64(cut)/float64(whole), failed, seeds)
	for _, wr := range [][2]int{{1, 1}, {2, 2}, {3, 1}} {
		lcut, lost := leaderlessWork(wr[0], wr[1], lsimScenario(t, "long-partition"), seeds)
		lwhole, _ := leaderlessWork(wr[0], wr[1], lsimScenario(t, "no-faults"), seeds)
		t.Logf("Leaderless variant W=%d R=%d answered %6d requests through the Partition and %6d with none: %5.1f%%; %d of %d runs lost an add or brought one back", wr[0], wr[1], lcut, lwhole, 100*float64(lcut)/float64(lwhole), lost, seeds)
	}
}
