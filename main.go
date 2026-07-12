package main

import (
	"bufio"
	"bytes"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"reditlite/resp"
)

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

type Entry struct {
	val []byte
	exp int64 // unix ms, 0 means no expiry
}

// Store holds all key/value data plus a version counter per key used by WATCH.
// A version is bumped every time a key is written or deleted.
type Store struct {
	mu       sync.RWMutex
	data     map[string]Entry
	versions map[string]uint64 // monotonically increasing write counter per key
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

// getVersion returns the current write-version for a key (0 if never written).
// Caller must NOT hold the lock.
func (s *Store) getVersion(key string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[key]
}

func (s *Store) set(key string, val []byte, ttlMs int64) {
	exp := int64(0)
	if ttlMs > 0 {
		exp = time.Now().UnixMilli() + ttlMs
	}
	s.mu.Lock()
	s.data[key] = Entry{val: val, exp: exp}
	s.versions[key]++
	s.mu.Unlock()
}

func (s *Store) del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0
	for _, k := range keys {
		if _, ok := s.data[k]; ok {
			delete(s.data, k)
			s.versions[k]++
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Per-connection transaction state
// ---------------------------------------------------------------------------

// txState tracks everything needed for a single client's MULTI/EXEC block.
type txState struct {
	active bool         // inside a MULTI block
	queue  []resp.Value // queued raw command arrays
	// watched maps key -> version at the time WATCH was called
	watched map[string]uint64
}

func newTxState() *txState {
	return &txState{watched: make(map[string]uint64)}
}

// isDirty returns true if any watched key has been modified since it was watched.
func (tx *txState) isDirty(st *Store) bool {
	for key, ver := range tx.watched {
		if st.getVersion(key) != ver {
			return true
		}
	}
	return false
}

// reset clears the transaction state but keeps the struct reusable.
func (tx *txState) reset() {
	tx.active = false
	tx.queue = tx.queue[:0]
	tx.watched = make(map[string]uint64)
}

// ---------------------------------------------------------------------------
// Main / connection handler
// ---------------------------------------------------------------------------

func main() {
	st := &Store{
		data:     make(map[string]Entry),
		versions: make(map[string]uint64),
	}

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
	tx := newTxState()

	for {
		val, err := resp.Read(r)
		if err != nil {
			return
		}
		if val.T != resp.Array || len(val.A) == 0 {
			_ = resp.WriteError(w, "ERR protocol error")
			_ = w.Flush()
			continue
		}

		cmd := strings.ToUpper(string(val.A[0].B))

		// Transaction-control commands are never queued — they execute immediately.
		switch cmd {
		case "MULTI":
			handleMulti(w, tx)
		case "EXEC":
			handleExec(w, st, tx)
		case "DISCARD":
			handleDiscard(w, tx)
		case "WATCH":
			handleWatch(w, st, tx, val.A)
		case "UNWATCH":
			handleUnwatch(w, tx)
		default:
			if tx.active {
				// Queue the command for later execution.
				tx.queue = append(tx.queue, val)
				_ = resp.WriteSimpleString(w, "QUEUED")
			} else {
				// Normal (non-transactional) execution.
				executeCommand(w, st, cmd, val.A)
			}
		}

		_ = w.Flush()
	}
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

// executeCommand runs a single command against the store and writes the result
// to w.  It is called both during normal operation and during EXEC replay.
func executeCommand(w *bufio.Writer, st *Store, cmd string, args []resp.Value) {
	switch cmd {
	case "PING":
		if len(args) > 1 {
			_ = resp.WriteBulk(w, args[1].B)
		} else {
			_ = resp.WriteSimpleString(w, "PONG")
		}
	case "ECHO":
		if len(args) != 2 || args[1].T != resp.BulkString {
			_ = resp.WriteError(w, "ERR wrong number of arguments for 'echo'")
			return
		}
		_ = resp.WriteBulk(w, args[1].B)
	case "SET":
		handleSet(w, st, args)
	case "GET":
		handleGet(w, st, args)
	case "DEL":
		handleDel(w, st, args)
	case "EXPIRE":
		handleExpire(w, st, args)
	case "TTL":
		handleTTL(w, st, args)
	default:
		_ = resp.WriteError(w, "ERR unknown command '"+cmd+"'")
	}
}

// ---------------------------------------------------------------------------
// Transaction handlers
// ---------------------------------------------------------------------------

func handleMulti(w *bufio.Writer, tx *txState) {
	if tx.active {
		_ = resp.WriteError(w, "ERR MULTI calls can not be nested")
		return
	}
	tx.active = true
	_ = resp.WriteSimpleString(w, "OK")
}

func handleExec(w *bufio.Writer, st *Store, tx *txState) {
	if !tx.active {
		_ = resp.WriteError(w, "ERR EXEC without MULTI")
		return
	}

	// If any watched key was modified, abort and return a null array.
	if tx.isDirty(st) {
		tx.reset()
		_ = resp.WriteNullArray(w)
		return
	}

	// Execute all queued commands and collect results.
	results := make([]resp.Value, 0, len(tx.queue))

	for _, queued := range tx.queue {
		cmd := strings.ToUpper(string(queued.A[0].B))

		// Capture the response for this command into a temporary buffer.
		capture := newCaptureWriter()
		bw := capture.asBufioWriter()
		executeCommand(bw, st, cmd, queued.A)
		_ = bw.Flush()

		// Parse the captured bytes back into a resp.Value so we can embed it
		// in the outer array response.
		v, parseErr := resp.Read(bufio.NewReader(capture.Reader()))
		if parseErr != nil {
			results = append(results, resp.Value{T: resp.Error, S: "ERR internal error"})
		} else {
			results = append(results, v)
		}
	}

	tx.reset()

	_ = resp.WriteArray(w, results)
}

func handleDiscard(w *bufio.Writer, tx *txState) {
	if !tx.active {
		_ = resp.WriteError(w, "ERR DISCARD without MULTI")
		return
	}
	tx.reset()
	_ = resp.WriteSimpleString(w, "OK")
}

func handleWatch(w *bufio.Writer, st *Store, tx *txState, args []resp.Value) {
	if tx.active {
		_ = resp.WriteError(w, "ERR WATCH inside MULTI is not allowed")
		return
	}
	if len(args) < 2 {
		_ = resp.WriteError(w, "ERR wrong number of arguments for 'watch'")
		return
	}
	for _, a := range args[1:] {
		key := string(a.B)
		tx.watched[key] = st.getVersion(key)
	}
	_ = resp.WriteSimpleString(w, "OK")
}

func handleUnwatch(w *bufio.Writer, tx *txState) {
	tx.watched = make(map[string]uint64)
	_ = resp.WriteSimpleString(w, "OK")
}

// ---------------------------------------------------------------------------
// captureWriter — thin buffer that lets us round-trip a RESP response
// ---------------------------------------------------------------------------

// captureWriter wraps a byte slice so executeCommand can write into it and we
// can then re-parse the result with resp.Read.
type captureWriter struct {
	buf []byte
	pos int
}

func newCaptureWriter() *captureWriter { return &captureWriter{} }

// bufio.Writer expects an io.Writer.
func (c *captureWriter) Write(p []byte) (int, error) {
	c.buf = append(c.buf, p...)
	return len(p), nil
}

// Reader returns an io.Reader over the captured bytes.
func (c *captureWriter) Reader() *bytes.Reader {
	return bytes.NewReader(c.buf)
}

// asBufioWriter wraps the captureWriter in a *bufio.Writer for use with our
// resp helpers (which all take *bufio.Writer).
func (c *captureWriter) asBufioWriter() *bufio.Writer {
	return bufio.NewWriter(c)
}

// ---------------------------------------------------------------------------
// Existing command handlers (unchanged except executeCommand calling them)
// ---------------------------------------------------------------------------

func handleSet(w *bufio.Writer, st *Store, args []resp.Value) {
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
	}
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
	}
	if e.exp == 0 {
		_ = resp.WriteInteger(w, -1)
		return
	}
	ms := e.exp - time.Now().UnixMilli()
	if ms < 0 {
		_ = resp.WriteInteger(w, -2)
		return
	}
	_ = resp.WriteInteger(w, ms/1000)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func parseIntMs(b []byte, mul int64) int64 {
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
