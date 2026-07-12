# redis-lite — Handoff

A lightweight Redis-compatible server written in Go. It speaks the RESP protocol on port **6379** and stores all data in-memory.

## Running

```
go run .
```

## Supported Commands

### General

| Command | Syntax | Description |
|---------|--------|-------------|
| `PING` | `PING [message]` | Returns `PONG`, or echoes `message` if provided. Useful for health checks. |
| `ECHO` | `ECHO message` | Returns `message` as a bulk string. |

### String / Key-Value

| Command | Syntax | Description |
|---------|--------|-------------|
| `SET` | `SET key value [EX seconds\|PX milliseconds]` | Stores `value` under `key`. Optional `EX` sets a TTL in seconds; `PX` sets it in milliseconds. |
| `GET` | `GET key` | Returns the value for `key`, or a null bulk string if the key does not exist or has expired. |
| `DEL` | `DEL key [key ...]` | Deletes one or more keys. Returns the number of keys that were actually removed. |

### Expiry

| Command | Syntax | Description |
|---------|--------|-------------|
| `EXPIRE` | `EXPIRE key seconds` | Sets a TTL (in seconds) on an existing key. Returns `1` if the key exists, `0` otherwise. |
| `TTL` | `TTL key` | Returns the remaining TTL of a key in seconds. Returns `-1` if the key has no expiry, `-2` if the key does not exist or has already expired. |

### Transactions

Transactions allow a sequence of commands to be queued and then executed atomically.

| Command | Syntax | Description |
|---------|--------|-------------|
| `MULTI` | `MULTI` | Starts a transaction block. All subsequent commands are queued rather than executed immediately. |
| `EXEC` | `EXEC` | Executes all queued commands atomically and returns an array of their results. If a watched key was modified by another client, the transaction is aborted and a null array is returned instead. |
| `DISCARD` | `DISCARD` | Aborts the current transaction and clears the command queue. |
| `WATCH` | `WATCH key [key ...]` | Marks one or more keys for optimistic locking. Must be called before `MULTI`. If any watched key is modified before `EXEC`, the transaction is aborted. |
| `UNWATCH` | `UNWATCH` | Clears all previously watched keys for the current connection. |

#### Transaction example

```
WATCH balance
MULTI
SET balance 200
EXEC
```

If another client modifies `balance` between `WATCH` and `EXEC`, `EXEC` returns a null array and none of the queued commands run.

#### Error behaviour inside transactions

- **Before EXEC** — if a queued command has an unknown name, it is still queued and returns an error when `EXEC` runs. The rest of the queue continues.
- **After EXEC** — runtime errors (e.g. wrong data type) are returned inline in the result array. Redis-lite does not roll back commands that already succeeded.

## Internals

- All data lives in a `map[string]Entry` protected by a `sync.RWMutex`.
- A background janitor goroutine runs every second to evict expired keys.
- `WATCH` is implemented via per-key monotonic version counters that are incremented on every write or delete.
- Transaction state (`MULTI`/`WATCH` queue) is per-connection and never shared between goroutines.

## Test Script

`tx_test.py` contains a Python smoke-test suite for the transaction commands. Start the server first, then run:

```
python3 tx_test.py
```
