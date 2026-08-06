# ssh -R over TLS, through Traefik, without `ssh`

Implement the **reverse port forwarding** semantics of `ssh -R` without the SSH
protocol, carrying the tunnel through **Traefik on port 443** (TLS).

```
ssh -R 6767:127.0.0.1:22 user@remote        # what we replicate
```

i.e. the **client** (behind NAT/firewall) exposes its local port `22`, and that
service becomes reachable on the **remote** machine at port `6767`. Everything
travels over the only publicly open port, `443`, handled by Traefik. Traefik
terminates TLS, so the link is encrypted end-to-end up to the remote backend.

```
                       443/TLS (Traefik terminates TLS)
   client (NAT)  ────────────────────────────────────►  remote
  Python 3.6        wss://tunnel.example.com/tunnel      Traefik ──► tunnel_server.py :8000 (ws)
   |                                                                   |
   | dials OUT, one persistent                                          | starts TCP listener
   | multiplexed WebSocket                                              | on 127.0.0.1:6767
   └─ local service :22 ◄──── bridge ──── inbound conn to :6767 ◄──────┘
```

## The key idea (and why `socat` alone cannot do this)

`ssh -R` works through NAT because the **client dials out** and the server
**multiplexes** many TCP streams over that single long-lived connection. To get
through Traefik (an L7 HTTP proxy) you must speak HTTP/WebSocket; raw TCP from
`socat`/`openssl` is dropped as a malformed request.

So the design is:

1. The **client** opens one **outbound** `wss://` WebSocket to Traefik and keeps
   it open. (This is what lets it sit behind NAT with only outbound 443.)
2. The **server** (behind Traefik) starts a TCP listener on the exposed
   `remotePort` (e.g. `6767`, loopback only).
3. For each inbound connection to `6767`, the server asks the client — over the
   single WebSocket — to connect to its local target (e.g. `127.0.0.1:22`).
4. Bytes for **all** streams are multiplexed over that one WebSocket with a tiny
   binary header (`cmd | stream_id | payload`). **No JSON, no base64** on the
   data path — that was the performance killer in the previous attempt.

This is the same model used by `chisel` / `frp` / `bore`, just reimplemented here
with **only the Python standard library** so the client runs on a bare Python 3.6
box with no `pip` and no internet.

## Files

| file | runs where | role |
|------|-----------|------|
| `wsutil.py`           | both   | stdlib WebSocket framing + multiplex message layer (shared) |
| `tunnel_server.py`    | remote | WS server (behind Traefik) + TCP listeners on exposed ports |
| `tunnel_client.py`    | client | WS dialer (`wss://` through Traefik) + local target bridge |
| `traefik/traefik-dynamic.yml` | remote | routes `Host/path` → the WS server, TLS termination |
| `test_loopback.py`    | either | end-to-end test (no TLS/Traefik) |

## Quick start

### 1. Remote: start the server (behind Traefik)
```bash
python3 tunnel_server.py --listen 127.0.0.1:8000 --bind 127.0.0.1 --secret S3CR3T
# --bind 127.0.0.1 keeps exposed remotePorts on loopback (no public ports except 80/443).
```

### 2. Remote: point Traefik at it
See `traefik/traefik-dynamic.yml`. The essential router:
```yaml
http:
  routers:
    tunnel-router:
      rule: "Host(`tunnel.example.com`) && PathPrefix(`/tunnel`)"
      entryPoints: [websecure]
      service: tunnel-service
      tls: { certResolver: myresolver }
  services:
    tunnel-service:
      loadBalancer:
        servers: [{ url: "http://127.0.0.1:8000" }]
```
Traefik proxies the WebSocket upgrade automatically and forwards the
`X-Tunnel-Secret` header through to the server.

### 3. Client: dial out and expose local SSH on the remote's 6767
```bash
python3 tunnel_client.py wss://tunnel.example.com/tunnel \
    --secret S3CR3T -R 6767:127.0.0.1:22
```
Multiple tunnels: add more `-R`, e.g. `-R 6768:127.0.0.1:5432`.

### 4. Use it
From the remote machine (or wherever `6767` is reachable):
```bash
ssh -p 6767 user@127.0.0.1     # lands on the client's SSH (port 22)
```

If you need `6767` reachable from the *outside* too (and only 443 is public),
add a Traefik **TCP router** with SNI routing on 443 that forwards a dedicated
SNI hostname to `127.0.0.1:6767`. That keeps the public surface at 443 only.

## Run the built-in test
```bash
python3 test_loopback.py        # needs Python 3.7+ for asyncio.run
```
It spins up server + client + an echo target in one process and checks small
echo, a 1 MB transfer (exercises the 8-byte WS length form + XOR masking), and
10 concurrent multiplexed streams.

## Performance notes
- **Binary framing**: data is sent as raw bytes in WebSocket binary frames with
  a 5-byte app header. No per-chunk JSON parsing, no base64 (the old approach
  paid ~2× size + two transcodings on every 4 KB chunk — the main reason it was
  slow).
- **Fast masking**: WebSocket client→server frames must be XOR-masked; this uses
  Python big-int XOR so the loop runs in C, not a per-byte Python loop.
- **Single connection, multiplexed**: one TLS handshake (Traefik's), then many
  streams share it — no per-stream TLS setup.
- **64 KB read buffers**, `drain()`-based backpressure per stream.
- For higher throughput still, or if you can run a static binary on the client,
  see `docs/chisel-shortcut.md` — a single Go binary does all of the above,
  faster, with the same Traefik setup.

## Security
- Traefik terminates TLS → the `wss://` leg is encrypted.
- `--secret` adds a shared-secret header (`X-Tunnel-Secret`) checked by the
  server; Traefik forwards it. You can additionally put Traefik `basicAuth`
  middleware on the router.
- Exposed remotePorts bind to `127.0.0.1` by default (not public). Make them
  public only via Traefik TCP+SNI on 443 if you truly need external reach.

## Reliability
- The client reconnects with exponential backoff (cap 60 s) and re-registers all
  `-R` mappings automatically on reconnect.
- A 30 s WebSocket PING keeps idle connections alive through proxies/NATs.
- On disconnect, all streams/listeners are cleaned up on both sides.

## Python 3.6 compatibility
- Uses only the standard library (`asyncio`, `ssl`, `socket`, `struct`,
  `hashlib`, `base64`).
- Avoids `asyncio.run` (3.7+) at runtime via a small fallback; f-strings are fine
  on 3.6.
- Deploy the client by copying just two files: `tunnel_client.py` + `wsutil.py`.
