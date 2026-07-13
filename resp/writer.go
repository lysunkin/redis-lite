package resp

import (
	"bufio"
	"fmt"
)

var crlf = []byte("\r\n")

func WriteSimpleString(w *bufio.Writer, s string) error {
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

func WriteError(w *bufio.Writer, s string) error {
	_, err := fmt.Fprintf(w, "-%s\r\n", s)
	return err
}

func WriteInteger(w *bufio.Writer, i int64) error {
	_, err := fmt.Fprintf(w, ":%d\r\n", i)
	return err
}

// WriteBulk writes a RESP bulk string. A nil slice writes a null bulk ($-1).
// The payload and the trailing CRLF are written as two separate calls to avoid
// allocating a new slice on every invocation.
func WriteBulk(w *bufio.Writer, b []byte) error {
	if b == nil {
		_, err := fmt.Fprint(w, "$-1\r\n")
		return err
	}
	if _, err := fmt.Fprintf(w, "$%d\r\n", len(b)); err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err := w.Write(crlf)
	return err
}

// WriteNullArray writes a RESP null array (*-1\r\n), used by EXEC when a
// transaction is aborted due to a dirty WATCH.
func WriteNullArray(w *bufio.Writer) error {
	_, err := fmt.Fprint(w, "*-1\r\n")
	return err
}

// WriteValue writes a single Value to w in RESP format. Arrays are written
// recursively.
func WriteValue(w *bufio.Writer, v Value) error {
	switch v.T {
	case SimpleString:
		return WriteSimpleString(w, v.S)
	case Error:
		return WriteError(w, v.S)
	case Integer:
		return WriteInteger(w, v.I)
	case BulkString:
		return WriteBulk(w, v.B)
	case Array:
		if v.A == nil {
			return WriteNullArray(w)
		}
		return WriteArray(w, v.A)
	default:
		return fmt.Errorf("unsupported RESP type %d", v.T)
	}
}

func WriteArray(w *bufio.Writer, arr []Value) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(arr)); err != nil {
		return err
	}
	for _, v := range arr {
		if err := WriteValue(w, v); err != nil {
			return err
		}
	}
	return nil
}
