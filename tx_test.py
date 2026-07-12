"""
Transaction smoke tests for redis-lite.

Usage:
    # Start the server first:
    #   go run .
    python3 tx_test.py
"""

import socket
import time


def cmd(*parts):
    """Build a RESP array command."""
    out = f"*{len(parts)}\r\n".encode()
    for p in parts:
        b = p.encode() if isinstance(p, str) else p
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def exchange(s, *commands):
    """Send one or more commands and return the raw response string."""
    payload = b"".join(cmd(*c) for c in commands)
    s.sendall(payload)
    time.sleep(0.15)
    return s.recv(4096).decode()


def conn(host="127.0.0.1", port=6379):
    s = socket.create_connection((host, port))
    s.settimeout(2)
    return s


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_basic_multi_exec():
    s = conn()
    r = exchange(s, ["MULTI"], ["SET", "foo", "1"], ["SET", "bar", "2"], ["EXEC"])
    assert "+OK" in r and "QUEUED" in r and "*2" in r, f"FAIL: {r!r}"
    print("Test 1 PASS (basic MULTI/EXEC):", repr(r))
    s.close()


def test_discard():
    s = conn()
    r = exchange(s, ["MULTI"], ["SET", "baz", "999"], ["DISCARD"], ["GET", "baz"])
    # baz was never committed, so GET must return nil bulk
    assert "$-1" in r, f"FAIL: {r!r}"
    print("Test 2 PASS (DISCARD):", repr(r))
    s.close()


def test_exec_without_multi():
    s = conn()
    r = exchange(s, ["EXEC"])
    assert "-ERR" in r, f"FAIL: {r!r}"
    print("Test 3 PASS (EXEC without MULTI):", repr(r))
    s.close()


def test_discard_without_multi():
    s = conn()
    r = exchange(s, ["DISCARD"])
    assert "-ERR" in r, f"FAIL: {r!r}"
    print("Test 4 PASS (DISCARD without MULTI):", repr(r))
    s.close()


def test_nested_multi():
    s = conn()
    r = exchange(s, ["MULTI"], ["MULTI"])
    assert "-ERR" in r, f"FAIL: {r!r}"
    exchange(s, ["DISCARD"])  # clean up
    print("Test 5 PASS (nested MULTI error):", repr(r))
    s.close()


def test_watch_clean_commit():
    """WATCH a key that nobody touches → EXEC should succeed."""
    s = conn()
    exchange(s, ["SET", "mykey", "10"])
    r = exchange(s, ["WATCH", "mykey"], ["MULTI"], ["SET", "mykey", "20"], ["EXEC"])
    assert "*1" in r, f"FAIL: {r!r}"
    print("Test 6 PASS (WATCH clean commit):", repr(r))
    s.close()


def test_watch_dirty_abort():
    """WATCH a key that another client modifies → EXEC returns null array."""
    s1 = conn()
    s2 = conn()

    exchange(s1, ["SET", "watched", "100"])

    s1.sendall(cmd("WATCH", "watched"))
    time.sleep(0.05); s1.recv(4096)

    s1.sendall(cmd("MULTI"))
    time.sleep(0.05); s1.recv(4096)

    s1.sendall(cmd("SET", "watched", "999"))
    time.sleep(0.05); s1.recv(4096)

    # Another client modifies the watched key before EXEC
    s2.sendall(cmd("SET", "watched", "777"))
    time.sleep(0.05); s2.recv(4096)

    s1.sendall(cmd("EXEC"))
    time.sleep(0.1)
    r = s1.recv(4096).decode()
    assert "*-1" in r, f"FAIL: {r!r}"
    print("Test 7 PASS (WATCH dirty abort → null array):", repr(r))

    s1.close()
    s2.close()


def test_exec_error_does_not_abort():
    """A runtime error inside EXEC should not prevent other commands from running."""
    s = conn()
    # Queue two commands: a valid SET and an unknown command.
    # The EXEC result array should have 2 elements; SET must succeed.
    r = exchange(
        s,
        ["SET", "strkey", "abc"],
        ["MULTI"],
        ["SET", "strkey", "xyz"],
        ["LPOP", "strkey"],   # unknown (or wrong-type) — returns an error inline
        ["EXEC"],
    )
    assert "*2" in r and "+OK" in r, f"FAIL: {r!r}"
    print("Test 8 PASS (runtime error inside EXEC doesn't abort):", repr(r))
    s.close()


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    test_basic_multi_exec()
    test_discard()
    test_exec_without_multi()
    test_discard_without_multi()
    test_nested_multi()
    test_watch_clean_commit()
    test_watch_dirty_abort()
    test_exec_error_does_not_abort()
    print("\nAll tests passed!")
