package main

import (
	"bufio"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"reditlite/resp"
)

type Entry struct {
	val []byte
	exp int64 // unix ms, 0 means no expiry
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

func (s *Store) get(key string) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	e, ok := s.data[key]
	if !ok {
		return Entry{}, false
	}
	if e.exp > 0 && time.Now().UnixMilli() > e.exp {
		return Entry{}, false
	}

	return e, true
}

func (s *Store) set(key string, val []byte, ttlMs int64) {
	exp := int64(0)
	if ttlMs > 0 {
		exp = time.Now().UnixMilli() + ttlMs
	}
	s.mu.Lock()
	s.data[key] = Entry{val: val, exp: exp}
	s.mu.Unlock()
}

func (s *Store) del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			n++
		}
	}
	return n
}

func main() {
	st := &Store{data: make(map[string]Entry)}

	// run janitor every 1 second
	startJanitor(st, time.Second)

	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("redis-lite listening on :6379")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept:", err)
			continue
		}
		go handleConn(conn, st)
	}
}

func handleConn(conn net.Conn, st *Store) {
	defer func() { _ = conn.Close() }()

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	for {
		val, err := resp.Read(r)
		if err != nil {
			return
		} // client closed or parse error
		if val.T != resp.Array || len(val.A) == 0 {
			_ = resp.WriteError(w, "ERR protocol error")
			_ = w.Flush()
			continue
		}

		// commands are bulk strings
		cmd := strings.ToUpper(string(val.A[0].B))

		switch cmd {
		case "PING":
			if len(val.A) > 1 {
				_ = resp.WriteBulk(w, val.A[1].B)
			} else {
				_ = resp.WriteSimpleString(w, "PONG")
			}
		case "ECHO":
			if len(val.A) != 2 || val.A[1].T != resp.BulkString {
				_ = resp.WriteError(w, "ERR wrong number of arguments for 'echo'")
				break
			}
			_ = resp.WriteBulk(w, val.A[1].B)
		case "SET":
			handleSet(w, st, val.A)
		case "GET":
			handleGet(w, st, val.A)
		case "DEL":
			handleDel(w, st, val.A)
		case "EXPIRE":
			handleExpire(w, st, val.A)
		case "TTL":
			handleTTL(w, st, val.A)
		default:
			_ = resp.WriteError(w, "ERR unknown command '"+cmd+"'")
		}
		_ = w.Flush()
	}
}

func handleSet(w *bufio.Writer, st *Store, args []resp.Value) {
	// SET key value [EX seconds|PX milliseconds]
	if len(args) < 3 {
		_ = resp.WriteError(w, "ERR wrong number of arguments for 'set'")
		return
	}
	key := string(args[1].B)
	val := args[2].B
	var ttlMs int64
	if len(args) >= 5 {
		opt := strings.ToUpper(string(args[3].B))
		if opt == "EX" {
			ttlMs = parseIntMs(args[4].B, 1000)
		}
		if opt == "PX" {
			ttlMs = parseIntMs(args[4].B, 1)
		}
	}
	st.set(key, val, ttlMs)
	_ = resp.WriteSimpleString(w, "OK")
}

func handleGet(w *bufio.Writer, st *Store, args []resp.Value) {
	if len(args) != 2 {
		_ = resp.WriteError(w, "ERR wrong number of arguments for 'get'")
		return
	}
	key := string(args[1].B)
	e, ok := st.get(key)
	if !ok {
		_ = resp.WriteBulk(w, nil)
		return
	} // null bulk
	_ = resp.WriteBulk(w, e.val)
}

func handleDel(w *bufio.Writer, st *Store, args []resp.Value) {
	if len(args) < 2 {
		_ = resp.WriteError(w, "ERR wrong number of arguments for 'del'")
		return
	}
	keys := make([]string, 0, len(args)-1)
	for _, a := range args[1:] {
		keys = append(keys, string(a.B))
	}
	n := st.del(keys...)
	_ = resp.WriteInteger(w, int64(n))
}

func handleExpire(w *bufio.Writer, st *Store, args []resp.Value) {
	if len(args) != 3 {
		_ = resp.WriteError(w, "ERR wrong number of arguments for 'expire'")
		return
	}
	key := string(args[1].B)
	secs := parseIntMs(args[2].B, 1000)

	st.mu.Lock()
	if e, ok := st.data[key]; ok {
		e.exp = time.Now().UnixMilli() + secs
		st.data[key] = e
		_ = resp.WriteInteger(w, 1)
	} else {
		_ = resp.WriteInteger(w, 0)
	}
	st.mu.Unlock()
}

func handleTTL(w *bufio.Writer, st *Store, args []resp.Value) {
	if len(args) != 2 {
		_ = resp.WriteError(w, "ERR wrong number of arguments for 'ttl'")
		return
	}

	st.mu.RLock()
	e, ok := st.data[string(args[1].B)]
	st.mu.RUnlock()

	if !ok {
		_ = resp.WriteInteger(w, -2)
		return
	} // key not found
	if e.exp == 0 {
		_ = resp.WriteInteger(w, -1)
		return
	} // no expire
	ms := e.exp - time.Now().UnixMilli()
	if ms < 0 {
		_ = resp.WriteInteger(w, -2)
		return
	}
	_ = resp.WriteInteger(w, ms/1000) // seconds like TTL
}

func parseIntMs(b []byte, mul int64) int64 {
	// naive parse; ignore errors for brevity
	var n int64
	for _, c := range b {
		n = n*10 + int64(c-'0')
	}
	return n * mul
}

func startJanitor(st *Store, every time.Duration) {
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for range t.C {
			now := time.Now().UnixMilli()
			st.mu.Lock()
			for k, e := range st.data {
				if e.exp > 0 && now > e.exp {
					delete(st.data, k)
				}
			}
			st.mu.Unlock()
		}
	}()
}
