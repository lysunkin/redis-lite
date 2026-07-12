package resp_test

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"redislite/resp"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parse feeds src through resp.Read and returns the value.
func parse(t *testing.T, src string) resp.Value {
	t.Helper()
	v, err := resp.Read(bufio.NewReader(strings.NewReader(src)))
	if err != nil {
		t.Fatalf("Read(%q) error: %v", src, err)
	}
	return v
}

// capture runs fn with a fresh *bufio.Writer and returns the raw bytes written.
func capture(t *testing.T, fn func(*bufio.Writer)) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	fn(w)
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush error: %v", err)
	}
	return buf.Bytes()
}

// ---------------------------------------------------------------------------
// resp.Read
// ---------------------------------------------------------------------------

func TestRead_SimpleString(t *testing.T) {
	v := parse(t, "+OK\r\n")
	if v.T != resp.SimpleString || v.S != "OK" {
		t.Fatalf("got T=%v S=%q, want SimpleString/OK", v.T, v.S)
	}
}

func TestRead_Error(t *testing.T) {
	v := parse(t, "-ERR something went wrong\r\n")
	if v.T != resp.Error || v.S != "ERR something went wrong" {
		t.Fatalf("got T=%v S=%q", v.T, v.S)
	}
}

func TestRead_Integer(t *testing.T) {
	for _, tc := range []struct {
		src  string
		want int64
	}{
		{":0\r\n", 0},
		{":42\r\n", 42},
		{":-1\r\n", -1},
		{":9223372036854775807\r\n", 9223372036854775807},
	} {
		v := parse(t, tc.src)
		if v.T != resp.Integer || v.I != tc.want {
			t.Errorf("Read(%q) = {T:%v I:%d}, want Integer/%d", tc.src, v.T, v.I, tc.want)
		}
	}
}

func TestRead_BulkString(t *testing.T) {
	v := parse(t, "$5\r\nhello\r\n")
	if v.T != resp.BulkString || string(v.B) != "hello" {
		t.Fatalf("got T=%v B=%q", v.T, v.B)
	}
}

func TestRead_BulkString_Empty(t *testing.T) {
	v := parse(t, "$0\r\n\r\n")
	if v.T != resp.BulkString || len(v.B) != 0 {
		t.Fatalf("got T=%v B=%q, want empty bulk", v.T, v.B)
	}
}

func TestRead_BulkString_Null(t *testing.T) {
	v := parse(t, "$-1\r\n")
	if v.T != resp.BulkString || v.B != nil {
		t.Fatalf("got T=%v B=%v, want null bulk", v.T, v.B)
	}
}

func TestRead_Array(t *testing.T) {
	// *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
	src := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	v := parse(t, src)
	if v.T != resp.Array || len(v.A) != 3 {
		t.Fatalf("got T=%v len=%d, want Array/3", v.T, len(v.A))
	}
	for i, want := range []string{"SET", "foo", "bar"} {
		if string(v.A[i].B) != want {
			t.Errorf("A[%d] = %q, want %q", i, v.A[i].B, want)
		}
	}
}

func TestRead_Array_Empty(t *testing.T) {
	v := parse(t, "*0\r\n")
	if v.T != resp.Array || len(v.A) != 0 {
		t.Fatalf("got T=%v len=%d, want Array/0", v.T, len(v.A))
	}
}

func TestRead_UnknownPrefix(t *testing.T) {
	_, err := resp.Read(bufio.NewReader(strings.NewReader("?foo\r\n")))
	if err == nil {
		t.Fatal("expected error for unknown prefix, got nil")
	}
}

// ---------------------------------------------------------------------------
// resp.Write*
// ---------------------------------------------------------------------------

func TestWriteSimpleString(t *testing.T) {
	got := capture(t, func(w *bufio.Writer) { _ = resp.WriteSimpleString(w, "PONG") })
	if string(got) != "+PONG\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteError(t *testing.T) {
	got := capture(t, func(w *bufio.Writer) { _ = resp.WriteError(w, "ERR oops") })
	if string(got) != "-ERR oops\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteInteger(t *testing.T) {
	for _, tc := range []struct{ n int64; want string }{
		{0, ":0\r\n"},
		{42, ":42\r\n"},
		{-1, ":-1\r\n"},
	} {
		got := capture(t, func(w *bufio.Writer) { _ = resp.WriteInteger(w, tc.n) })
		if string(got) != tc.want {
			t.Errorf("WriteInteger(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestWriteBulk(t *testing.T) {
	got := capture(t, func(w *bufio.Writer) { _ = resp.WriteBulk(w, []byte("hello")) })
	if string(got) != "$5\r\nhello\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteBulk_Null(t *testing.T) {
	got := capture(t, func(w *bufio.Writer) { _ = resp.WriteBulk(w, nil) })
	if string(got) != "$-1\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteNullArray(t *testing.T) {
	got := capture(t, func(w *bufio.Writer) { _ = resp.WriteNullArray(w) })
	if string(got) != "*-1\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriteArray(t *testing.T) {
	arr := []resp.Value{
		{T: resp.SimpleString, S: "OK"},
		{T: resp.Integer, I: 7},
		{T: resp.BulkString, B: []byte("hello")},
		{T: resp.BulkString, B: nil}, // null bulk
	}
	got := capture(t, func(w *bufio.Writer) { _ = resp.WriteArray(w, arr) })
	want := "*4\r\n+OK\r\n:7\r\n$5\r\nhello\r\n$-1\r\n"
	if string(got) != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// Round-trip: write then read back
// ---------------------------------------------------------------------------

func TestRoundTrip_BulkString(t *testing.T) {
	original := []byte("hello, world!")
	raw := capture(t, func(w *bufio.Writer) { _ = resp.WriteBulk(w, original) })
	v, err := resp.Read(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if string(v.B) != string(original) {
		t.Fatalf("round-trip: got %q, want %q", v.B, original)
	}
}

func TestRoundTrip_Array(t *testing.T) {
	arr := []resp.Value{
		{T: resp.SimpleString, S: "QUEUED"},
		{T: resp.Integer, I: 3},
	}
	raw := capture(t, func(w *bufio.Writer) { _ = resp.WriteArray(w, arr) })
	v, err := resp.Read(bufio.NewReader(bytes.NewReader(raw)))
	if err != nil {
		t.Fatal(err)
	}
	if v.T != resp.Array || len(v.A) != 2 {
		t.Fatalf("got T=%v len=%d", v.T, len(v.A))
	}
	if v.A[0].S != "QUEUED" || v.A[1].I != 3 {
		t.Fatalf("unexpected contents: %+v", v.A)
	}
}
