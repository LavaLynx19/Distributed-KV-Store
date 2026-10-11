package server_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func txnBody(conds map[string]float64, value string, keys ...string) string {
	body := `{"if":[`
	first := true
	for k, v := range conds {
		if !first {
			body += ","
		}
		first = false
		body += fmt.Sprintf(`{"key":%q,"version":%d}`, k, uint64(v))
	}
	body += `],"writes":[`
	for i, k := range keys {
		if i > 0 {
			body += ","
		}
		body += fmt.Sprintf(`{"op":"put","key":%q,"value":%q}`, k, value)
	}
	return body + "]}"
}

// A Transaction whose keys are in two Groups is carried out by both, and
// gives its keys one version (A§12).
func TestATransactionAcrossGroups(t *testing.T) {
	urls := startStore(t, 4)
	// Slot 0 starts in Group 1 and Slot 1 in Group 2.
	a, b := keyInSlot(0, 0), keyInSlot(1, 0)
	// Give Group 1 a longer Log than Group 2, so their own versions differ.
	for i := range 12 {
		eventually(t, 200, "PUT", urls[1]+"/v1/kv/"+keyInSlot(0, 1), `{"value":"`+strconv.Itoa(i)+`"}`)
	}
	done := eventually(t, 200, "POST", urls[2]+"/v1/txn", txnBody(nil, "one", a, b))
	version := done.body["version"].(float64)
	for _, key := range []string{a, b} {
		got := eventually(t, 200, "GET", urls[3]+"/v1/kv/"+key, "")
		if got.body["value"] != "one" || got.body["version"] != version {
			t.Fatalf("%s after the Transaction: %+v, want version %v", key, got.body, version)
		}
	}
	// A later write in the Group with the shorter Log gets a higher version.
	later := eventually(t, 200, "PUT", urls[1]+"/v1/kv/"+b, `{"value":"two"}`)
	if later.body["version"].(float64) <= version {
		t.Fatalf("a write after the Transaction got version %v, not above %v", later.body["version"], version)
	}

	// A condition that fails in one Group stops the write in the other.
	refused := eventually(t, 409, "POST", urls[4]+"/v1/txn", txnBody(map[string]float64{b: version}, "three", a, b))
	failed, _ := refused.body["failed"].([]any)
	if refused.body["reason"] != "version_mismatch" || len(failed) != 1 || failed[0].(map[string]any)["key"] != b {
		t.Fatalf("a failed condition: %+v", refused.body)
	}
	if got := eventually(t, 200, "GET", urls[3]+"/v1/kv/"+a, ""); got.body["value"] != "one" {
		t.Fatalf("%s was written by a Transaction whose condition failed: %+v", a, got.body)
	}

	// In a Session, a retry through another Node is answered as before and
	// carried out once.
	open := eventually(t, 200, "POST", urls[1]+"/v1/sessions", "")
	session := []string{"Session-Id", strconv.Itoa(int(open.body["session"].(float64))), "Request-Seq", "1"}
	first := eventually(t, 200, "POST", urls[2]+"/v1/txn", txnBody(nil, "four", a, b), append(session, "Session-Register", "1")...)
	eventually(t, 200, "PUT", urls[1]+"/v1/kv/"+a, `{"value":"five"}`)
	retry := eventually(t, 200, "POST", urls[3]+"/v1/txn", txnBody(nil, "four", a, b), session...)
	if retry.body["version"] != first.body["version"] {
		t.Fatalf("the retry got version %v, the first attempt %v", retry.body["version"], first.body["version"])
	}
	if got := eventually(t, 200, "GET", urls[4]+"/v1/kv/"+a, ""); got.body["value"] != "five" {
		t.Fatalf("the retry was carried out a second time: %+v", got.body)
	}
}

// Clients fight over the same keys in three Groups. Every Transaction
// writes one value to all its keys, so at any moment the keys it shares
// with the last one to commit hold the same value: after the fight, all
// three keys do. Nothing is left Locked or undecided.
func TestTransactionsAcrossGroupsUnderContention(t *testing.T) {
	urls := startStoreOf(t, 5, 3)
	keys := []string{keyInSlot(0, 0), keyInSlot(1, 0), keyInSlot(2, 0)}
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed, conflicts := 0, 0
	for c := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 15 {
				body, age := txnBody(nil, fmt.Sprintf("c%d-%d", c, i), keys...), 0.0
				for try := 0; try < 200; try++ {
					send := body
					if age != 0 {
						send = fmt.Sprintf(`{"age":%d,%s`, int64(age), body[1:])
					}
					a := call(t, "POST", urls[1+(c+try)%5]+"/v1/txn", send)
					if a.code == 200 {
						mu.Lock()
						committed++
						mu.Unlock()
						break
					}
					if a.body["reason"] == "txn_conflict" {
						age, _ = a.body["age"].(float64)
						mu.Lock()
						conflicts++
						mu.Unlock()
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	if committed != 90 {
		t.Fatalf("%d of 90 Transactions committed (%d aborted over a conflict on the way)", committed, conflicts)
	}
	first := eventually(t, 200, "GET", urls[1]+"/v1/kv/"+keys[0], "")
	for _, key := range keys[1:] {
		got := eventually(t, 200, "GET", urls[2]+"/v1/kv/"+key, "")
		if got.body["value"] != first.body["value"] || got.body["version"] != first.body["version"] {
			t.Fatalf("%s holds %+v and %s holds %+v", keys[0], first.body, key, got.body)
		}
	}
	until(t, 5*time.Second, "nothing Locked or undecided", func() bool {
		for _, url := range urls {
			st := call(t, "GET", url+"/v1/status", "")
			groups, _ := st.body["groups"].([]any)
			for _, g := range groups {
				g := g.(map[string]any)
				if g["locked"] != nil || g["open"] != nil {
					return false
				}
			}
		}
		return true
	})
	t.Logf("90 committed, %d aborted over a conflict on the way", conflicts)
}
