package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

func Read(r *bufio.Reader) (Value, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch prefix {
	case '+': // Simple String
		line, _ := readLine(r)
		return Value{T: SimpleString, S: line}, nil
	case '-': // Error
		line, _ := readLine(r)
		return Value{T: Error, S: line}, nil
	case ':': // Integer
		line, _ := readLine(r)
		n, _ := strconv.ParseInt(line, 10, 64)
		return Value{T: Integer, I: n}, nil
	case '$': // Bulk String
		nstr, _ := readLine(r)
		n, _ := strconv.Atoi(nstr)
		if n == -1 {
			return Value{T: BulkString, B: nil}, nil
		} // Null bulk
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return Value{}, err
		}
		return Value{T: BulkString, B: buf[:n]}, nil
	case '*': // Array
		nstr, _ := readLine(r)
		n, _ := strconv.Atoi(nstr)
		arr := make([]Value, n)
		for i := 0; i < n; i++ {
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
