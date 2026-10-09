// Command kvctl is the operator's tool (A§7.3): it shows a Group's Members
// and adds or removes one.
//
//	kvctl -nodes http://127.0.0.1:8001,http://127.0.0.1:8002 status
//	kvctl -nodes … add 4
//	kvctl -nodes … remove 2
//
// -nodes lists the client API of any Nodes, Members or Spares. A change is
// sent to whichever of them leads.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	nodes := flag.String("nodes", "", "client API URLs of some Nodes, comma-separated")
	wait := flag.Duration("wait", 30*time.Second, "how long to keep trying a change that is refused for now")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: kvctl -nodes URL[,URL…] status | add ID | remove ID")
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
