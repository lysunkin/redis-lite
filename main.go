package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"redislite/resp"
)

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

// entry holds a single key's value and optional expiry.
type entry struct {
	val []byte
	exp int64 // unix ms; 0 means no expiry
}

// Store is the in-memory key/value store. All operations are safe for
// concurrent use.
//
// versions is kept as a separate map from data so that version numbers survive
// key deletions. This is what allows WATCH to detect that a key was deleted
// between WATCH and EXEC: the version is bumped on delete and persists even
// though the data entry is gone. The two maps are always written together
// inside a Lock, which the Store's own methods enforce — callers must use the
// provided methods and never access the fields directly.
type Store struct {
	mu       sync.RWMutex
	data     map[string]entry
	versions map[string]uint64 // monotonically increasing; never deleted
}

func newStoreImpl() *Store {
	return &Store{
		data:     make(map[string]entry),
		versions: make(map[string]uint64),
	}
}

// get returns the entry for key if it exists and has not expired.
func (s *Store) get(key string) (entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lockedGet(key)
}

// lockedGet is the read path without locking; callers must hold at least RLock.
func (s *Store) lockedGet(key string) (entry, bool) {
	e, ok := s.data[key]
	if !ok {
		return entry{}, false
	}
	if e.exp > 0 && time.Now().UnixMilli() > e.exp {
		return entry{}, false
	}
	return e, true
}

// getVersion returns the current write-version for a key (0 if never written).
func (s *Store) getVersion(key string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.versions[key]
}

// set writes key with the given value and TTL. ttlMs == 0 means no expiry.
func (s *Store) set(key string, val []byte, ttlMs int64) {
	exp := int64(0)
	if ttlMs > 0 {
		exp = time.Now().UnixMilli() + ttlMs
	}
	s.mu.Lock()
	s.data[key] = entry{val: val, exp: exp}
	s.versions[key]++
	s.mu.Unlock()
}

// del removes one or more keys and returns the count of keys that existed.
// The version is bumped for each deleted key so that WATCH detects the change
// even after the data entry is gone.
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

// expire sets a TTL (in milliseconds) on an existing key. Returns 1 if the key
// existed, 0 otherwise. The version is bumped so that WATCH detects the change.
func (s *Store) expire(key string, ttlMs int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		return 0
	}
	e.exp = time.Now().UnixMilli() + ttlMs
	s.data[key] = e
	s.versions[key]++
	return 1
}

// ttl returns the remaining TTL for a key in seconds, or the Redis sentinel
// values: -1 (no expiry), -2 (key missing / expired). The entire read is done
// under a single RLock to avoid a TOCTOU race between reading the entry and
// calling time.Now().
func (s *Store) ttl(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lockedGet(key)
	if !ok {
		return -2
	}
	if e.exp == 0 {
		return -1
	}
	ms := e.exp - time.Now().UnixMilli()
	if ms <= 0 {
		return -2
	}
	return ms / 1000
}

// ---------------------------------------------------------------------------
// Per-connection transaction state
// ---------------------------------------------------------------------------

// txState tracks everything needed for a single client's MULTI/EXEC block.
// It is purely per-connection and never shared between goroutines.
type txState struct {
	active  bool              // inside a MULTI block
	queue   []resp.Value      // queued raw command arrays
	watched map[string]uint64 // key → version at WATCH time
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

// reset clears all transaction state, reusing the struct allocation.
func (tx *txState) reset() {
	tx.active = false
	tx.queue = tx.queue[:0]
	tx.watched = make(map[string]uint64)
}

// ---------------------------------------------------------------------------
// Main / connection handling
// ---------------------------------------------------------------------------

// maxConns caps the number of simultaneously open client connections.
const maxConns = 1000

func main() {
	st := newStoreImpl()
	startJanitor(st, time.Second)

	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("redis-lite listening on :6379")

	sem := make(chan struct{}, maxConns)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept:", err)
			continue
		}

		// Try to acquire a slot without blocking. If full, reject immediately.
		select {
		case sem <- struct{}{}:
		default:
			log.Println("max connections reached, rejecting")
			_ = conn.Close()
			continue
		}

		go func() {
			defer func() { <-sem }()
			handleConn(conn, st)
		}()
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
			if err := resp.WriteError(w, "ERR protocol error"); err != nil {
				return
			}
			if err := w.Flush(); err != nil {
				return
			}
			continue
		}

		cmd := strings.ToUpper(string(val.A[0].B))

		// Transaction-control commands are never queued — they execute immediately.
		var writeErr error
		switch cmd {
		case "MULTI":
			writeErr = handleMulti(w, tx)
		case "EXEC":
			writeErr = handleExec(w, st, tx)
		case "DISCARD":
			writeErr = handleDiscard(w, tx)
		case "WATCH":
			writeErr = handleWatch(w, st, tx, val.A)
		case "UNWATCH":
			writeErr = handleUnwatch(w, tx)
		default:
			if tx.active {
				tx.queue = append(tx.queue, val)
				writeErr = resp.WriteSimpleString(w, "QUEUED")
			} else {
				result := executeCommand(st, cmd, val.A)
				writeErr = resp.WriteValue(w, result)
			}
		}

		if writeErr != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Command dispatch
// ---------------------------------------------------------------------------

// executeCommand runs a single command against the store and returns the result
// as a resp.Value. It is called both during normal operation and during EXEC
// replay. Returning a value (rather than writing directly) eliminates the
// encode-decode round-trip that was previously needed inside handleExec.
func executeCommand(st *Store, cmd string, args []resp.Value) resp.Value {
	switch cmd {
	case "PING":
		if len(args) > 1 {
			return resp.Value{T: resp.BulkString, B: args[1].B}
		}
		return resp.Value{T: resp.SimpleString, S: "PONG"}

	case "ECHO":
		if len(args) != 2 || args[1].T != resp.BulkString {
			return errReply("ERR wrong number of arguments for 'echo'")
		}
		return resp.Value{T: resp.BulkString, B: args[1].B}

	case "SET":
		return handleSet(st, args)
	case "GET":
		return handleGet(st, args)
	case "DEL":
		return handleDel(st, args)
	case "EXPIRE":
		return handleExpire(st, args)
	case "TTL":
		return handleTTL(st, args)

	default:
		return errReply("ERR unknown command '" + cmd + "'")
	}
}

// errReply is a convenience constructor for error values.
func errReply(msg string) resp.Value {
	return resp.Value{T: resp.Error, S: msg}
}

// intReply is a convenience constructor for integer values.
func intReply(n int64) resp.Value {
	return resp.Value{T: resp.Integer, I: n}
}

// okReply is the canonical +OK response.
func okReply() resp.Value {
	return resp.Value{T: resp.SimpleString, S: "OK"}
}

// nullBulk is the canonical null bulk string ($-1).
func nullBulk() resp.Value {
	return resp.Value{T: resp.BulkString, B: nil}
}

// ---------------------------------------------------------------------------
// Transaction handlers — now return error instead of taking *bufio.Writer
// directly for the write path, but still need the writer for composite
// responses (EXEC array). Simple responses go through the caller in handleConn.
// ---------------------------------------------------------------------------

func handleMulti(w *bufio.Writer, tx *txState) error {
	if tx.active {
		return resp.WriteError(w, "ERR MULTI calls can not be nested")
	}
	tx.active = true
	return resp.WriteSimpleString(w, "OK")
}

func handleExec(w *bufio.Writer, st *Store, tx *txState) error {
	if !tx.active {
		return resp.WriteError(w, "ERR EXEC without MULTI")
	}

	// If any watched key was modified, abort and return a null array.
	if tx.isDirty(st) {
		tx.reset()
		return resp.WriteNullArray(w)
	}

	// Execute all queued commands and collect results directly as resp.Values —
	// no encode/decode round-trip needed.
	results := make([]resp.Value, len(tx.queue))
	for i, queued := range tx.queue {
		cmd := strings.ToUpper(string(queued.A[0].B))
		results[i] = executeCommand(st, cmd, queued.A)
	}

	tx.reset()
	return resp.WriteArray(w, results)
}

func handleDiscard(w *bufio.Writer, tx *txState) error {
	if !tx.active {
		return resp.WriteError(w, "ERR DISCARD without MULTI")
	}
	tx.reset()
	return resp.WriteSimpleString(w, "OK")
}

func handleWatch(w *bufio.Writer, st *Store, tx *txState, args []resp.Value) error {
	if tx.active {
		return resp.WriteError(w, "ERR WATCH inside MULTI is not allowed")
	}
	if len(args) < 2 {
		return resp.WriteError(w, "ERR wrong number of arguments for 'watch'")
	}
	for _, a := range args[1:] {
		key := string(a.B)
		tx.watched[key] = st.getVersion(key)
	}
	return resp.WriteSimpleString(w, "OK")
}

func handleUnwatch(w *bufio.Writer, tx *txState) error {
	tx.watched = make(map[string]uint64)
	return resp.WriteSimpleString(w, "OK")
}

// ---------------------------------------------------------------------------
// Command handlers — return resp.Value, no I/O
// ---------------------------------------------------------------------------

func handleSet(st *Store, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return errReply("ERR wrong number of arguments for 'set'")
	}
	key := string(args[1].B)
	val := args[2].B

	var ttlMs int64
	if len(args) >= 5 {
		opt := strings.ToUpper(string(args[3].B))
		n, err := parseInteger(args[4].B)
		if err != nil || n <= 0 {
			return errReply("ERR value is not an integer or out of range")
		}
		switch opt {
		case "EX":
			ttlMs = n * 1000
		case "PX":
			ttlMs = n
		}
	}

	st.set(key, val, ttlMs)
	return okReply()
}

func handleGet(st *Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return errReply("ERR wrong number of arguments for 'get'")
	}
	e, ok := st.get(string(args[1].B))
	if !ok {
		return nullBulk()
	}
	return resp.Value{T: resp.BulkString, B: e.val}
}

func handleDel(st *Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return errReply("ERR wrong number of arguments for 'del'")
	}
	keys := make([]string, len(args)-1)
	for i, a := range args[1:] {
		keys[i] = string(a.B)
	}
	return intReply(int64(st.del(keys...)))
}

func handleExpire(st *Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return errReply("ERR wrong number of arguments for 'expire'")
	}
	n, err := parseInteger(args[2].B)
	if err != nil || n < 0 {
		return errReply("ERR value is not an integer or out of range")
	}
	return intReply(int64(st.expire(string(args[1].B), n*1000)))
}

func handleTTL(st *Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return errReply("ERR wrong number of arguments for 'ttl'")
	}
	return intReply(st.ttl(string(args[1].B)))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseInteger parses a decimal integer from a byte slice. Returns an error for
// any non-numeric or out-of-range input, replacing the old silent-corruption bug.
func parseInteger(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty integer")
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer or out of range")
	}
	return n, nil
}

// startJanitor launches a background goroutine that evicts expired keys in
// small batches (up to batchSize random keys per tick) to avoid holding the
// store lock for an unbounded duration.
func startJanitor(st *Store, every time.Duration) {
	const batchSize = 20
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for range t.C {
			evictBatch(st, batchSize)
		}
	}()
}

func evictBatch(st *Store, max int) {
	now := time.Now().UnixMilli()
	st.mu.Lock()
	defer st.mu.Unlock()

	n := 0
	for k, e := range st.data {
		if n >= max {
			break
		}
		if e.exp > 0 && now > e.exp {
			delete(st.data, k)
			st.versions[k]++
			n++
		}
	}
}
