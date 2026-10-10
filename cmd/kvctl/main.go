// Command kvctl is the operator's tool (A§7.3): it shows a Group's Members
// and adds or removes one.
//
//	kvctl -nodes http://127.0.0.1:8001,http://127.0.0.1:8002 status
//	kvctl -nodes … add 4
//	kvctl -nodes … remove 2
//	kvctl -nodes … table          (a store with several Groups, A§11)
//	kvctl -nodes … move SLOT GROUP
//
// -nodes lists the client API of any Nodes, Members or Spares. A change is
// sent to whichever of them leads.
//
// It also performs Unsafe recovery (A§6.6), which works on a stopped
// Member's files and talks to nobody:
//
//	kvctl unsafe-recover -data DIR -members 2,5 [-confirm]
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"distributed-kv-store/internal/core"
	"distributed-kv-store/internal/raft"
	"distributed-kv-store/internal/storage"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "unsafe-recover" {
		if err := unsafeRecover(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "kvctl:", err)
			os.Exit(1)
		}
		return
	}
	nodes := flag.String("nodes", "", "client API URLs of some Nodes, comma-separated")
	wait := flag.Duration("wait", 30*time.Second, "how long to keep trying a change that is refused for now")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl -nodes URL[,URL…] status | add ID | remove ID | table | move SLOT GROUP")
		fmt.Fprintln(os.Stderr, "       kvctl unsafe-recover -data DIR -members ID[,ID…] [-confirm]")
		flag.PrintDefaults()
	}
	flag.Parse()
	urls := strings.Split(*nodes, ",")
	if *nodes == "" || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	httpc := &http.Client{Timeout: 40 * time.Second}

	var err error
	switch cmd := flag.Arg(0); {
	case cmd == "status" && flag.NArg() == 1:
		err = status(httpc, urls)
	case (cmd == "add" || cmd == "remove") && flag.NArg() == 2:
		var id uint64
		if id, err = strconv.ParseUint(flag.Arg(1), 10, 64); err != nil || id == 0 {
			err = fmt.Errorf("%q isn't a Node id", flag.Arg(1))
			break
		}
		err = change(httpc, urls, cmd, id, *wait)
	case cmd == "table" && flag.NArg() == 1:
		err = table(httpc, urls)
	case cmd == "move" && flag.NArg() == 3:
		slot, err1 := strconv.ParseUint(flag.Arg(1), 10, 16)
		group, err2 := strconv.ParseUint(flag.Arg(2), 10, 32)
		if err1 != nil || err2 != nil {
			err = errors.New("move takes a Slot and a Group, as numbers")
			break
		}
		err = move(httpc, urls, slot, group, *wait)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		os.Exit(1)
	}
}

type statusAnswer struct {
	ID         uint64   `json:"id"`
	Role       string   `json:"role"`
	Term       uint64   `json:"term"`
	Leader     uint64   `json:"leader"`
	Commit     uint64   `json:"commit"`
	Recovering bool     `json:"recovering"`
	Members    []uint64 `json:"members"`
}

// status prints what each Node says about itself and the Group.
func status(httpc *http.Client, urls []string) error {
	reached := 0
	for _, u := range urls {
		resp, err := httpc.Get(u + "/v1/status")
		if err != nil {
			fmt.Printf("%-28s down\n", u)
			continue
		}
		var st statusAnswer
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("%-28s unreadable answer: %v\n", u, err)
			continue
		}
		reached++
		note := ""
		if !contains(st.Members, st.ID) {
			note = "  (not a Member)"
		}
		if st.Recovering {
			note += "  (Recovering)"
		}
		fmt.Printf("%-28s node %d  %-9s term %d  commit %d  members %v%s\n", u, st.ID, st.Role, st.Term, st.Commit, st.Members, note)
	}
	if reached == 0 {
		return fmt.Errorf("no Node answered")
	}
	return nil
}

// alreadySo reports whether the Node at url already has the Member list the
// change asked for.
func alreadySo(httpc *http.Client, url, verb string, id uint64) bool {
	resp, err := httpc.Get(url + "/v1/status")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	var st statusAnswer
	if json.NewDecoder(resp.Body).Decode(&st) != nil || len(st.Members) == 0 {
		return false
	}
	return contains(st.Members, id) == (verb == "add")
}

func contains(list []uint64, id uint64) bool {
	for _, m := range list {
		if m == id {
			return true
		}
	}
	return false
}

type changeAnswer struct {
	Members []uint64 `json:"members"`
	Reason  string   `json:"reason"`
	Message string   `json:"message"`
	Leader  string   `json:"leader"`
}

// change asks the Leader to add or remove one Member. It follows hints to
// the Leader, and keeps trying while the answer is "not now".
func change(httpc *http.Client, urls []string, verb string, id uint64, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	target, next := urls[0], 1
	for {
		var req *http.Request
		var err error
		if verb == "add" {
			req, err = http.NewRequest(http.MethodPost, target+"/v1/admin/members", bytes.NewReader([]byte(fmt.Sprintf(`{"id":%d}`, id))))
		} else {
			req, err = http.NewRequest(http.MethodDelete, target+"/v1/admin/members/"+strconv.FormatUint(id, 10), nil)
		}
		if err != nil {
			return err
		}
		var a changeAnswer
		code := 0
		if resp, err := httpc.Do(req); err == nil {
			raw, _ := io.ReadAll(resp.Body) // an unreadable answer is retried like no answer
			resp.Body.Close()
			_ = json.Unmarshal(raw, &a)
			code = resp.StatusCode
		}
		switch {
		case code == http.StatusOK:
			fmt.Printf("done: the Group's Members are now %v\n", a.Members)
			return nil
		case a.Reason == "invalid" && alreadySo(httpc, target, verb, id):
			// An earlier attempt went through and its answer was lost.
			fmt.Printf("done: node %d was already %s\n", id, map[string]string{"add": "a Member", "remove": "not a Member"}[verb])
			return nil
		case a.Reason == "invalid" || a.Reason == "member_unreachable":
			return fmt.Errorf("%s: %s", a.Reason, a.Message)
		case time.Now().After(deadline):
			if a.Reason == "" {
				a.Reason = "no answer"
			}
			return fmt.Errorf("gave up after %s; the last answer was %q. The change may or may not have been made: run status", wait, a.Reason)
		case a.Reason == "not_leader" && a.Leader != "":
			target = a.Leader
		default:
			// No Leader known there, a change already under way, a timeout,
			// or nobody home: try the next Node after a moment.
			target, next = urls[next%len(urls)], next+1
			time.Sleep(200 * time.Millisecond)
		}
	}
}

// unsafeRecover forces the Member list on a stopped Member's disk, after
// saying what that does. Without -confirm it only says.
func unsafeRecover(args []string) error {
	fs := flag.NewFlagSet("unsafe-recover", flag.ExitOnError)
	data := fs.String("data", "", "the stopped Member's data directory")
	list := fs.String("members", "", "ids of the Members that survive: 2,5")
	confirm := fs.Bool("confirm", false, "do it. Without this, only say what would be done")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *data == "" || *list == "" {
		return errors.New("unsafe-recover needs -data and -members")
	}
	var members []core.NodeID
	for _, field := range strings.Split(*list, ",") {
		id, err := strconv.ParseUint(strings.TrimSpace(field), 10, 64)
		if err != nil || id == 0 {
			return fmt.Errorf("-members: %q isn't a Node id", field)
		}
		members = append(members, core.NodeID(id))
	}
	if _, err := os.Stat(*data); err != nil {
		return fmt.Errorf("-data: %w", err)
	}

	// Opening the directory repairs it if it is damaged, exactly as starting
	// the Member would, so looking first changes nothing a start wouldn't.
	store, stored, err := storage.Open(*data, 0)
	if err != nil {
		return err
	}
	if err := store.Close(); err != nil {
		return err
	}
	last, lastTerm := core.Index(0), core.Term(0)
	if stored.Snapshot != nil {
		last, lastTerm = stored.Snapshot.Index, stored.Snapshot.Term
	}
	if n := len(stored.Entries); n > 0 {
		last, lastTerm = stored.Entries[n-1].Index, stored.Entries[n-1].Term
	}
	fmt.Printf("Unsafe recovery of the Member in %s\n", *data)
	fmt.Printf("  It holds Entries up to %d (Term %d). The latest Term it saw is %d.\n", last, lastTerm, stored.HardState.Term)
	if was, known := raft.StoredMembers(stored); known {
		var removed []core.NodeID
		for _, m := range was {
			if !containsID(members, m) {
				removed = append(removed, m)
			}
		}
		fmt.Printf("  Its Member list is %v. It becomes %v, which discards %v.\n", was, members, removed)
	} else {
		fmt.Printf("  Its Member list is the one the Group started with. It becomes %v.\n", members)
	}
	if stored.Damaged {
		fmt.Println("  It found damage on its disk and hasn't recovered: it may be missing Entries it acknowledged. It will vote all the same.")
	}
	fmt.Printf("  DISCARDED: every write the Group Committed after Entry %d, unless another survivor holds it.\n", last)
	fmt.Println("  Run this on every survivor with the same list. The survivor holding the most will lead.")
	fmt.Println("  Never start a discarded Member again with its old data: it would form a second Group.")
	if !*confirm {
		fmt.Println("Nothing was changed. Run again with -confirm to do it.")
		return nil
	}
	if _, err := storage.ForceMembers(storage.OSFS{}, *data, members, storage.Options{}); err != nil {
		return err
	}
	fmt.Println("Done. Start the Member.")
	return nil
}

func containsID(list []core.NodeID, id core.NodeID) bool {
	for _, m := range list {
		if m == id {
			return true
		}
	}
	return false
}

type tableAnswer struct {
	Version uint64 `json:"version"`
	Slots   []struct {
		Slot     int    `json:"slot"`
		Group    uint64 `json:"group"`
		Epoch    uint64 `json:"epoch"`
		MovingTo uint64 `json:"moving_to"`
	} `json:"slots"`
}

// fetchTable asks the Nodes in turn for the Slot table until one answers.
func fetchTable(httpc *http.Client, urls []string) (tableAnswer, error) {
	for _, u := range urls {
		resp, err := httpc.Get(u + "/v1/table")
		if err != nil {
			continue
		}
		var t tableAnswer
		err = json.NewDecoder(resp.Body).Decode(&t)
		resp.Body.Close()
		if err == nil && resp.StatusCode == http.StatusOK {
			return t, nil
		}
	}
	return tableAnswer{}, errors.New("no Node gave the Slot table")
}

// table prints which Group owns each Slot.
func table(httpc *http.Client, urls []string) error {
	t, err := fetchTable(httpc, urls)
	if err != nil {
		return err
	}
	fmt.Printf("table version %d\n", t.Version)
	byGroup := map[uint64][]int{}
	for _, row := range t.Slots {
		byGroup[row.Group] = append(byGroup[row.Group], row.Slot)
		if row.MovingTo != 0 {
			fmt.Printf("  Slot %d is moving from Group %d to Group %d\n", row.Slot, row.Group, row.MovingTo)
		}
	}
	for g := uint64(1); len(byGroup[g]) > 0 || g <= uint64(len(byGroup)); g++ {
		fmt.Printf("  Group %d owns %d Slots: %v\n", g, len(byGroup[g]), byGroup[g])
	}
	return nil
}

// move asks for a Slot to be moved to a Group and waits until the table
// shows it there.
func move(httpc *http.Client, urls []string, slot, group uint64, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	body := fmt.Sprintf(`{"slot":%d,"to":%d}`, slot, group)
	began := time.Now()
	for asked := false; ; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			return fmt.Errorf("gave up after %s: run table to see where the Move stands", wait)
		}
		if !asked {
			for _, u := range urls {
				resp, err := httpc.Post(u+"/v1/admin/moves", "application/json", strings.NewReader(body))
				if err != nil {
					continue
				}
				var a changeAnswer
				raw, _ := io.ReadAll(resp.Body) // an unreadable answer is retried like no answer
				resp.Body.Close()
				_ = json.Unmarshal(raw, &a)
				if resp.StatusCode == http.StatusBadRequest {
					return fmt.Errorf("%s: %s", a.Reason, a.Message)
				}
				if resp.StatusCode == http.StatusOK {
					asked = true
					break
				}
			}
			continue
		}
		t, err := fetchTable(httpc, urls)
		if err != nil || int(slot) >= len(t.Slots) {
			continue
		}
		if row := t.Slots[slot]; row.Group == group && row.MovingTo == 0 {
			fmt.Printf("done: Slot %d is owned by Group %d at Epoch %d, after %s\n", slot, group, row.Epoch, time.Since(began).Round(time.Millisecond))
			return nil
		}
	}
}
