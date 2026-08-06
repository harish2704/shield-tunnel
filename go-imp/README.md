# Go implementation of the ssh-over-TLS reverse tunnel

This is a Go reimplementation of the Python tunnel in the parent directory. It
is **wire-compatible** with the Python client/server (`(wsutil.py`,
`tunnel_client.py`, `tunnel_server.py`) — the two ecosystems interoperate
freely:

```
Go server  +  Python client      ✅ verified
Python server  +  Go client     ✅ verified
```

(verified: small echo, 1 MB transfer — exercises the 8-byte WS length form and
WebSocket masking — and 10 concurrent multiplexed streams.)

## Why

The Python client was written for a locked-down Python 3.6 box. When you *can*
ship a binary, the Go version is faster (compiled, goroutines), still has zero
runtime dependencies (ships a static binary), and exposes the same Traefik
deployment.

## Layout

```
go-imp/
  go.mod
  wsutil/        protocol.go frame.go conn.go handshake.go  (stdlib WebSocket + multiplex layer)
  tunnel/        server.go client.go                          (session logic; importable + tested)
  tunnel/tunnel_test.go                                       (loopback integration test)
  cmd/tunnel-server/main.go   cmd/tunnel-client/main.go       (CLIs)
  interop_check.py                                             (cross-interop probe: echo + verify)
  README.md
```

Same protocol as the Python version — one WebSocket, binary frames with a
5-byte app header (`cmd | uint32 stream id | payload`), commands
`OPEN/READY/OPEN_FAIL/DATA/CLOSE/REGISTER/ERROR`, `X-Tunnel-Secret` header,
WebSocket PING keepalive. See `wsutil/protocol.go` and the parent README for
details.

## Build

```bash
cd go-imp
go build -o tunnel-server ./cmd/tunnel-server
go build -o tunnel-client ./cmd/tunnel-client
# or, to install all commands into $GOBIN:
go install ./...
```

No external modules — only the standard library. `go.mod` has zero require
lines.

## Usage

### Server (remote, behind Traefik)
```bash
./tunnel-server -listen 127.0.0.1:8000 -bind 127.0.0.1 -secret S3CR3T
# -bind 127.0.0.1 keeps exposed remotePorts on loopback (only 80/443 public).
```
Point Traefik at it with the same `traefik/traefik-dynamic.yml` from the parent
directory (route `Host/path -> http://127.0.0.1:8000`).

### Client (behind NAT)
```bash
./tunnel-client -secret S3CR3T -R 6767:127.0.0.1:22 wss://tunnel.example.com/tunnel
# extra tunnels: add more -R, e.g. -R 6768:127.0.0.1:5432
# self-signed TLS cert: add -insecure
```

### Use it
```bash
ssh -p 6767 user@127.0.0.1     # on the remote, lands on the client's SSH
```

## Tests

```bash
cd go-imp
go test ./tunnel/ -run TestLoopback -v        # Go server + Go client, in-process
```

Cross-interop (one Go, one Python) is run by hand with `interop_check.py`; see
`interop_check.py --help`-less usage:
```bash
python3 interop_check.py <echo_port> <remote_port>
```
while you start one server and one client from either implementation on
`127.0.0.1:8000` and `-R <remote_port>:127.0.0.1:<echo_port>`.

## Performance notes

- Stdlib WebSocket with a 5-byte multiplex header; data is raw bytes — no JSON,
  no base64 (the same design choice that made the Python version fast).
- One persistent connection shared by all streams — one TLS handshake (Traefik's).
- Writes serialized by a mutex; per-stream 64 KB read buffers with natural TCP
  backpressure.
- `go vet` clean; framed correctly for the 126/127 length forms and masked
  client→server frames.

Mix and match: run the Python client where only Python is available and the Go
server where you want speed (or vice versa) without changing the wire format.