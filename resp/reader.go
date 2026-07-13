package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Read parses one RESP value from r. All parse errors are propagated; none are
// silently swallowed.
func Read(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case '+': // Simple String
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{T: SimpleString, S: line}, nil

	case '-': // Error
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{T: Error, S: line}, nil

	case ':': // Integer
		line, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("invalid integer %q: %w", line, err)
		}
		return Value{T: Integer, I: n}, nil

	case '$': // Bulk String
		nstr, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.Atoi(nstr)
		if err != nil {
			return Value{}, fmt.Errorf("invalid bulk length %q: %w", nstr, err)
		}
		if n == -1 {
			return Value{T: BulkString, B: nil}, nil // null bulk
		}
		if n < 0 {
			return Value{}, fmt.Errorf("invalid bulk length %d", n)
		}
		buf := make([]byte, n+2) // +2 for trailing \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return Value{}, err
		}
		return Value{T: BulkString, B: buf[:n]}, nil

	case '*': // Array
		nstr, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		n, err := strconv.Atoi(nstr)
		if err != nil {
			return Value{}, fmt.Errorf("invalid array length %q: %w", nstr, err)
		}
		if n == -1 {
			return Value{T: Array, A: nil}, nil // null array
		}
		if n < 0 {
			return Value{}, fmt.Errorf("invalid array length %d", n)
		}
		arr := make([]Value, n)
		for i := range arr {
			v, err := Read(r)
			if err != nil {
				return Value{}, err
			}
			arr[i] = v
		}
		return Value{T: Array, A: arr}, nil

	default:
		return Value{}, errors.New("unknown RESP prefix")
	}
}

func readLine(r *bufio.Reader) (string, error) {
	b, err := r.ReadBytes('\n')
	if err != nil {
		return "", err
	}
	b = bytes.TrimSuffix(b, []byte("\r\n"))
	return string(b), nil
}
