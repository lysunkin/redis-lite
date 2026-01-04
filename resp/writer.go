package resp

import (
	"bufio"
	"fmt"
)

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

func WriteBulk(w *bufio.Writer, b []byte) error {
	if b == nil {
		_, err := fmt.Fprint(w, "$-1\r\n")
		return err
	}
	_, err := fmt.Fprintf(w, "$%d\r\n", len(b))
	if err != nil {
		return err
	}
	_, err = w.Write(append(b, '\r', '\n'))
	return err
}

func WriteArray(w *bufio.Writer, arr []Value) error {
	_, err := fmt.Fprintf(w, "*%d\r\n", len(arr))
	if err != nil {
		return err
	}
	for _, v := range arr {
		switch v.T {
		case SimpleString:
			if err = WriteSimpleString(w, v.S); err != nil {
				return err
			}
		case Error:
			if err = WriteError(w, v.S); err != nil {
				return err
			}
		case Integer:
			if err = WriteInteger(w, v.I); err != nil {
				return err
			}
		case BulkString:
			if err = WriteBulk(w, v.B); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported nested type")
		}
	}
	return nil
}
