package resp

type Type int

const (
	SimpleString Type = iota
	Error
	Integer
	BulkString
	Array
)

type Value struct {
	T Type
	S string
	I int64
	B []byte
	A []Value
}
