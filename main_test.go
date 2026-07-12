package main

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"

	"redislite/resp"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newStore returns a clean, empty Store ready for testing.
func newStore() *Store {
	return &Store{
		data:     make(map[string]Entry),
		versions: make(map[string]uint64),
	}
}

// run sends args as a command to executeCommand and returns the parsed response.
func run(t *testing.T, st *Store, args ...string) resp.Value {
	t.Helper()
	vals := makeArgs(args...)
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	executeCommand(w, st, strings.ToUpper(args[0]), vals)
	_ = w.Flush()
	v, err := resp.Read(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("run(%v): parse error: %v (raw: %q)", args, err, buf.Bytes())
	}
	return v
}

// makeArgs converts string slices into []resp.Value bulk strings.
func makeArgs(args ...string) []resp.Value {
	vals := make([]resp.Value, len(args))
	for i, a := range args {
		vals[i] = resp.Value{T: resp.BulkString, B: []byte(a)}
	}
	return vals
}

// runTx calls a transaction handler and returns the parsed response.
func runMulti(t *testing.T, tx *txState) resp.Value {
	t.Helper()
	return txRun(t, func(w *bufio.Writer) { handleMulti(w, tx) })
}

func runExec(t *testing.T, st *Store, tx *txState) resp.Value {
	t.Helper()
	return txRun(t, func(w *bufio.Writer) { handleExec(w, st, tx) })
}

func runDiscard(t *testing.T, tx *txState) resp.Value {
	t.Helper()
	return txRun(t, func(w *bufio.Writer) { handleDiscard(w, tx) })
}

func runWatch(t *testing.T, st *Store, tx *txState, keys ...string) resp.Value {
	t.Helper()
	args := append([]resp.Value{{T: resp.BulkString, B: []byte("WATCH")}}, makeArgs(keys...)...)
	return txRun(t, func(w *bufio.Writer) { handleWatch(w, st, tx, args) })
}

func runUnwatch(t *testing.T, tx *txState) resp.Value {
	t.Helper()
	return txRun(t, func(w *bufio.Writer) { handleUnwatch(w, tx) })
}

func txRun(t *testing.T, fn func(*bufio.Writer)) resp.Value {
	t.Helper()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	fn(w)
	_ = w.Flush()
	v, err := resp.Read(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("txRun: parse error: %v (raw: %q)", err, buf.Bytes())
	}
	return v
}

// assertOK fails if v is not a SimpleString "OK".
func assertOK(t *testing.T, v resp.Value) {
	t.Helper()
	if v.T != resp.SimpleString || v.S != "OK" {
		t.Fatalf("expected +OK, got T=%v S=%q", v.T, v.S)
	}
}

// assertError fails if v is not an Error containing substr.
func assertError(t *testing.T, v resp.Value, substr string) {
	t.Helper()
	if v.T != resp.Error || !strings.Contains(v.S, substr) {
		t.Fatalf("expected error containing %q, got T=%v S=%q", substr, v.T, v.S)
	}
}

// assertBulk fails if v is not a BulkString with the given value.
func assertBulk(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.T != resp.BulkString || string(v.B) != want {
		t.Fatalf("expected bulk %q, got T=%v B=%q", want, v.T, v.B)
	}
}

// assertNullBulk fails if v is not a null BulkString.
func assertNullBulk(t *testing.T, v resp.Value) {
	t.Helper()
	if v.T != resp.BulkString || v.B != nil {
		t.Fatalf("expected null bulk, got T=%v B=%q", v.T, v.B)
	}
}

// assertInteger fails if v is not an Integer with the given value.
func assertInteger(t *testing.T, v resp.Value, want int64) {
	t.Helper()
	if v.T != resp.Integer || v.I != want {
		t.Fatalf("expected integer %d, got T=%v I=%d", want, v.T, v.I)
	}
}

// assertNullArray fails if v is not a null array (*-1).
func assertNullArray(t *testing.T, v resp.Value) {
	t.Helper()
	// resp.Read represents *-1 as an Array with nil slice.
	if v.T != resp.Array || v.A != nil {
		t.Fatalf("expected null array (*-1), got T=%v A=%v", v.T, v.A)
	}
}

// ---------------------------------------------------------------------------
// Store unit tests
// ---------------------------------------------------------------------------

func TestStore_SetGet(t *testing.T) {
	st := newStore()
	st.set("k", []byte("v"), 0)
	e, ok := st.get("k")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(e.val) != "v" {
		t.Fatalf("got %q, want %q", e.val, "v")
	}
}

func TestStore_GetMissing(t *testing.T) {
	st := newStore()
	_, ok := st.get("missing")
	if ok {
		t.Fatal("expected key to be absent")
	}
}

func TestStore_Overwrite(t *testing.T) {
	st := newStore()
	st.set("k", []byte("first"), 0)
	st.set("k", []byte("second"), 0)
	e, _ := st.get("k")
	if string(e.val) != "second" {
		t.Fatalf("expected overwrite, got %q", e.val)
	}
}

func TestStore_Del_Existing(t *testing.T) {
	st := newStore()
	st.set("a", []byte("1"), 0)
	st.set("b", []byte("2"), 0)
	n := st.del("a", "b", "missing")
	if n != 2 {
		t.Fatalf("expected 2 deletions, got %d", n)
	}
	if _, ok := st.get("a"); ok {
		t.Fatal("key 'a' should be gone")
	}
}

func TestStore_Del_Missing(t *testing.T) {
	st := newStore()
	n := st.del("nope")
	if n != 0 {
		t.Fatalf("expected 0, got %d", n)
	}
}

func TestStore_Expiry(t *testing.T) {
	st := newStore()
	st.set("k", []byte("v"), 50) // 50 ms TTL
	if _, ok := st.get("k"); !ok {
		t.Fatal("key should still be alive")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := st.get("k"); ok {
		t.Fatal("key should have expired")
	}
}

func TestStore_VersionBumpsOnSet(t *testing.T) {
	st := newStore()
	v0 := st.getVersion("k")
	st.set("k", []byte("a"), 0)
	v1 := st.getVersion("k")
	st.set("k", []byte("b"), 0)
	v2 := st.getVersion("k")
	if !(v0 < v1 && v1 < v2) {
		t.Fatalf("versions not strictly increasing: %d %d %d", v0, v1, v2)
	}
}

func TestStore_VersionBumpsOnDel(t *testing.T) {
	st := newStore()
	st.set("k", []byte("a"), 0)
	vBefore := st.getVersion("k")
	st.del("k")
	vAfter := st.getVersion("k")
	if vAfter <= vBefore {
		t.Fatalf("version should increase on delete: before=%d after=%d", vBefore, vAfter)
	}
}

// ---------------------------------------------------------------------------
// PING / ECHO
// ---------------------------------------------------------------------------

func TestPing_NoArg(t *testing.T) {
	v := run(t, newStore(), "PING")
	if v.T != resp.SimpleString || v.S != "PONG" {
		t.Fatalf("got T=%v S=%q", v.T, v.S)
	}
}

func TestPing_WithMessage(t *testing.T) {
	v := run(t, newStore(), "PING", "hello")
	assertBulk(t, v, "hello")
}

func TestEcho(t *testing.T) {
	v := run(t, newStore(), "ECHO", "world")
	assertBulk(t, v, "world")
}

func TestEcho_MissingArg(t *testing.T) {
	v := run(t, newStore(), "ECHO")
	assertError(t, v, "ERR")
}

// ---------------------------------------------------------------------------
// SET / GET
// ---------------------------------------------------------------------------

func TestSetGet(t *testing.T) {
	st := newStore()
	assertOK(t, run(t, st, "SET", "foo", "bar"))
	assertBulk(t, run(t, st, "GET", "foo"), "bar")
}

func TestGet_Missing(t *testing.T) {
	assertNullBulk(t, run(t, newStore(), "GET", "nope"))
}

func TestSet_OverwritesExisting(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "k", "old")
	run(t, st, "SET", "k", "new")
	assertBulk(t, run(t, st, "GET", "k"), "new")
}

func TestSet_WithEX(t *testing.T) {
	st := newStore()
	// EX 1 → expires in 1 second; key should be alive immediately
	assertOK(t, run(t, st, "SET", "k", "v", "EX", "1"))
	assertBulk(t, run(t, st, "GET", "k"), "v")
}

func TestSet_WithPX(t *testing.T) {
	st := newStore()
	// PX 50 → expires in 50 ms
	assertOK(t, run(t, st, "SET", "k", "v", "PX", "50"))
	assertBulk(t, run(t, st, "GET", "k"), "v")
	time.Sleep(60 * time.Millisecond)
	assertNullBulk(t, run(t, st, "GET", "k"))
}

func TestSet_MissingArgs(t *testing.T) {
	assertError(t, run(t, newStore(), "SET", "k"), "ERR")
}

func TestGet_MissingArgs(t *testing.T) {
	assertError(t, run(t, newStore(), "GET"), "ERR")
}

// ---------------------------------------------------------------------------
// DEL
// ---------------------------------------------------------------------------

func TestDel_Single(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "a", "1")
	assertInteger(t, run(t, st, "DEL", "a"), 1)
	assertNullBulk(t, run(t, st, "GET", "a"))
}

func TestDel_Multiple(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "a", "1")
	run(t, st, "SET", "b", "2")
	assertInteger(t, run(t, st, "DEL", "a", "b"), 2)
}

func TestDel_PartialMissing(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "a", "1")
	assertInteger(t, run(t, st, "DEL", "a", "missing"), 1)
}

func TestDel_AllMissing(t *testing.T) {
	assertInteger(t, run(t, newStore(), "DEL", "x", "y"), 0)
}

func TestDel_MissingArgs(t *testing.T) {
	assertError(t, run(t, newStore(), "DEL"), "ERR")
}

// ---------------------------------------------------------------------------
// EXPIRE / TTL
// ---------------------------------------------------------------------------

func TestExpire_ExistingKey(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "k", "v")
	assertInteger(t, run(t, st, "EXPIRE", "k", "10"), 1)
}

func TestExpire_MissingKey(t *testing.T) {
	assertInteger(t, run(t, newStore(), "EXPIRE", "nope", "10"), 0)
}

func TestTTL_NoExpiry(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "k", "v")
	assertInteger(t, run(t, st, "TTL", "k"), -1)
}

func TestTTL_WithExpiry(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "k", "v", "EX", "100")
	v := run(t, st, "TTL", "k")
	// TTL should be close to 100 s; anything in [99, 100] is correct
	if v.T != resp.Integer || v.I < 99 || v.I > 100 {
		t.Fatalf("expected TTL ~100, got %d", v.I)
	}
}

func TestTTL_MissingKey(t *testing.T) {
	assertInteger(t, run(t, newStore(), "TTL", "gone"), -2)
}

func TestTTL_ExpiredKey(t *testing.T) {
	st := newStore()
	run(t, st, "SET", "k", "v", "PX", "30")
	time.Sleep(40 * time.Millisecond)
	assertInteger(t, run(t, st, "TTL", "k"), -2)
}

// ---------------------------------------------------------------------------
// Unknown command
// ---------------------------------------------------------------------------

func TestUnknownCommand(t *testing.T) {
	assertError(t, run(t, newStore(), "FOOBAR"), "ERR unknown command")
}

// ---------------------------------------------------------------------------
// Transaction: txState helpers
// ---------------------------------------------------------------------------

func TestTxState_IsDirty_Clean(t *testing.T) {
	st := newStore()
	st.set("k", []byte("v"), 0)
	tx := newTxState()
	tx.watched["k"] = st.getVersion("k")
	if tx.isDirty(st) {
		t.Fatal("should not be dirty: key unchanged")
	}
}

func TestTxState_IsDirty_AfterSet(t *testing.T) {
	st := newStore()
	st.set("k", []byte("v"), 0)
	tx := newTxState()
	tx.watched["k"] = st.getVersion("k")
	st.set("k", []byte("changed"), 0) // bump version
	if !tx.isDirty(st) {
		t.Fatal("should be dirty: key was modified")
	}
}

func TestTxState_IsDirty_AfterDel(t *testing.T) {
	st := newStore()
	st.set("k", []byte("v"), 0)
	tx := newTxState()
	tx.watched["k"] = st.getVersion("k")
	st.del("k")
	if !tx.isDirty(st) {
		t.Fatal("should be dirty: key was deleted")
	}
}

func TestTxState_Reset(t *testing.T) {
	tx := newTxState()
	tx.active = true
	tx.queue = append(tx.queue, resp.Value{T: resp.SimpleString, S: "x"})
	tx.watched["k"] = 5
	tx.reset()
	if tx.active || len(tx.queue) != 0 || len(tx.watched) != 0 {
		t.Fatal("reset did not clear txState")
	}
}

// ---------------------------------------------------------------------------
// MULTI / EXEC / DISCARD
// ---------------------------------------------------------------------------

func TestMulti_OK(t *testing.T) {
	tx := newTxState()
	assertOK(t, runMulti(t, tx))
	if !tx.active {
		t.Fatal("tx should be active after MULTI")
	}
}

func TestMulti_Nested(t *testing.T) {
	tx := newTxState()
	runMulti(t, tx)
	assertError(t, runMulti(t, tx), "nested")
}

func TestDiscard_OK(t *testing.T) {
	tx := newTxState()
	runMulti(t, tx)
	assertOK(t, runDiscard(t, tx))
	if tx.active {
		t.Fatal("tx should not be active after DISCARD")
	}
}

func TestDiscard_WithoutMulti(t *testing.T) {
	tx := newTxState()
	assertError(t, runDiscard(t, tx), "DISCARD without MULTI")
}

func TestExec_WithoutMulti(t *testing.T) {
	tx := newTxState()
	assertError(t, runExec(t, newStore(), tx), "EXEC without MULTI")
}

func TestExec_EmptyQueue(t *testing.T) {
	st := newStore()
	tx := newTxState()
	runMulti(t, tx)
	v := runExec(t, st, tx)
	if v.T != resp.Array || len(v.A) != 0 {
		t.Fatalf("expected empty array, got T=%v len=%d", v.T, len(v.A))
	}
	if tx.active {
		t.Fatal("tx should be inactive after EXEC")
	}
}

func TestExec_ExecutesQueue(t *testing.T) {
	st := newStore()
	tx := newTxState()
	runMulti(t, tx)

	// Queue: SET foo hello, SET bar world
	for _, args := range [][]string{{"SET", "foo", "hello"}, {"SET", "bar", "world"}} {
		arr := makeArgs(args...)
		tx.queue = append(tx.queue, resp.Value{T: resp.Array, A: arr})
	}

	v := runExec(t, st, tx)
	if v.T != resp.Array || len(v.A) != 2 {
		t.Fatalf("expected array of 2, got T=%v len=%d", v.T, len(v.A))
	}
	assertOK(t, v.A[0])
	assertOK(t, v.A[1])

	// Values should actually be in the store
	assertBulk(t, run(t, st, "GET", "foo"), "hello")
	assertBulk(t, run(t, st, "GET", "bar"), "world")
}

func TestExec_InlineError_DoesNotAbort(t *testing.T) {
	st := newStore()
	tx := newTxState()
	runMulti(t, tx)

	// Queue: SET k v (valid), FOOBAR (unknown command)
	tx.queue = append(tx.queue,
		resp.Value{T: resp.Array, A: makeArgs("SET", "k", "v")},
		resp.Value{T: resp.Array, A: makeArgs("FOOBAR")},
	)

	v := runExec(t, st, tx)
	if v.T != resp.Array || len(v.A) != 2 {
		t.Fatalf("expected 2-element array, got T=%v len=%d", v.T, len(v.A))
	}
	assertOK(t, v.A[0])
	assertError(t, v.A[1], "ERR")

	// SET must have committed despite the following error
	assertBulk(t, run(t, st, "GET", "k"), "v")
}

func TestExec_ResetsStateAfterCommit(t *testing.T) {
	st := newStore()
	tx := newTxState()
	runMulti(t, tx)
	runExec(t, st, tx)
	if tx.active || len(tx.queue) != 0 || len(tx.watched) != 0 {
		t.Fatal("txState should be fully reset after EXEC")
	}
}

// ---------------------------------------------------------------------------
// WATCH / UNWATCH
// ---------------------------------------------------------------------------

func TestWatch_OK(t *testing.T) {
	st := newStore()
	st.set("k", []byte("v"), 0)
	tx := newTxState()
	assertOK(t, runWatch(t, st, tx, "k"))
	if _, ok := tx.watched["k"]; !ok {
		t.Fatal("key should be in watched map")
	}
}

func TestWatch_InsideMulti(t *testing.T) {
	st := newStore()
	tx := newTxState()
	runMulti(t, tx)
	assertError(t, runWatch(t, st, tx, "k"), "WATCH inside MULTI")
}

func TestWatch_NoKeys(t *testing.T) {
	st := newStore()
	tx := newTxState()
	// Pass only the command name, no key args
	args := []resp.Value{{T: resp.BulkString, B: []byte("WATCH")}}
	v := txRun(t, func(w *bufio.Writer) { handleWatch(w, st, tx, args) })
	assertError(t, v, "ERR")
}

func TestWatch_DirtyAbort(t *testing.T) {
	st := newStore()
	st.set("mykey", []byte("100"), 0)

	tx := newTxState()
	runWatch(t, st, tx, "mykey")
	runMulti(t, tx)

	// Another "client" modifies the key
	st.set("mykey", []byte("999"), 0)

	v := runExec(t, st, tx)
	assertNullArray(t, v)
}

func TestWatch_CleanCommit(t *testing.T) {
	st := newStore()
	st.set("mykey", []byte("100"), 0)

	tx := newTxState()
	runWatch(t, st, tx, "mykey")
	runMulti(t, tx)
	tx.queue = append(tx.queue,
		resp.Value{T: resp.Array, A: makeArgs("SET", "mykey", "200")},
	)

	v := runExec(t, st, tx)
	if v.T != resp.Array || len(v.A) != 1 {
		t.Fatalf("expected 1-element array, got T=%v len=%d", v.T, len(v.A))
	}
	assertOK(t, v.A[0])
	assertBulk(t, run(t, st, "GET", "mykey"), "200")
}

func TestWatch_MultipleKeys_OneDirty(t *testing.T) {
	st := newStore()
	st.set("a", []byte("1"), 0)
	st.set("b", []byte("2"), 0)

	tx := newTxState()
	runWatch(t, st, tx, "a", "b")
	runMulti(t, tx)

	st.set("b", []byte("modified"), 0) // only b is dirty

	assertNullArray(t, runExec(t, st, tx))
}

func TestUnwatch_ClearsWatched(t *testing.T) {
	st := newStore()
	tx := newTxState()
	runWatch(t, st, tx, "k1", "k2")
	if len(tx.watched) != 2 {
		t.Fatalf("expected 2 watched keys, got %d", len(tx.watched))
	}
	assertOK(t, runUnwatch(t, tx))
	if len(tx.watched) != 0 {
		t.Fatalf("expected 0 watched keys after UNWATCH, got %d", len(tx.watched))
	}
}

func TestUnwatch_ThenDirtyKeyNoLongerAborts(t *testing.T) {
	st := newStore()
	st.set("k", []byte("v"), 0)

	tx := newTxState()
	runWatch(t, st, tx, "k")
	runUnwatch(t, tx) // clear the watch

	// Modify the key — but since we unwatched, it shouldn't matter
	st.set("k", []byte("changed"), 0)

	runMulti(t, tx)
	tx.queue = append(tx.queue,
		resp.Value{T: resp.Array, A: makeArgs("SET", "k", "final")},
	)

	v := runExec(t, st, tx)
	if v.T != resp.Array || len(v.A) != 1 {
		t.Fatalf("expected commit after UNWATCH, got T=%v A=%v", v.T, v.A)
	}
	assertOK(t, v.A[0])
}
