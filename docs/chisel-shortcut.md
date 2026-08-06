# Shortcut: use `chisel` if you can run a static binary on the client

If the client machine can run a single static binary (no Python needed), **chisel**
is the fastest path: it implements exactly the model above (reverse tunnels over
one multiplexed WebSocket, behind any HTTP/TLS reverse proxy) and is
battle-tested. This doc is the recommended production option when the "Python
3.6 only" constraint does not apply.

> chisel is a fast TCP/UDP tunnel over HTTP, secured with TLS, multiplexing many
> connections over one WebSocket. The `R:` prefix means a **reverse** tunnel
> (server-side listener), exactly like `ssh -R`.

## Architecture (same as the Python version)

```
client (NAT) --wss:443--> Traefik --http--> chisel server --TCP 6767--> anyone on remote
chisel client: R:6767:127.0.0.1:22
```

## 1. Run chisel server on the remote (behind Traefik)

Download: <https://github.com/jpillora/chisel/releases> (static Go binary).

```bash
# Listen on 127.0.0.1:8000 (Traefik forwards 443 -> 8000).
# --reverse allows clients to register reverse (R:) tunnels.
# --auth sets the shared secret (user:pass).
chisel server --host 127.0.0.1 --port 8000 --reverse --auth user:S3CR3T
```

## 2. Traefik config

```yaml
http:
  routers:
    chisel-router:
      rule: "Host(`tunnel.example.com`)"
      entryPoints: [websecure]
      service: chisel-service
      tls: { certResolver: myresolver }
  services:
    chisel-service:
      loadBalancer:
        servers: [{ url: "http://127.0.0.1:8000" }]
```

(Use `Host(...)` only, or add `PathPrefix`; chisel handles its own paths. Traefik
proxies the WebSocket upgrade automatically.)

## 3. Run chisel client on the client machine

```bash
# Reverse: expose the client's 127.0.0.1:22 as remote port 6767
chisel client --auth user:S3CR3T wss://tunnel.example.com R:6767:127.0.0.1:22
```

`R:6767:127.0.0.1:22` ⇒ the remote listens on `6767`, and traffic is bridged to
the client's `127.0.0.1:22`. Add more `R:` entries for more tunnels.

## 4. Use it

```bash
ssh -p 6767 user@127.0.0.1     # from the remote, reaches the client's SSH
```

## chisel vs. the Python implementation in this repo

| | chisel | this repo (stdlib Python) |
|---|---|---|
| Client needs | a static binary | only Python 3.6+ (2 files) |
| Performance | highest (Go) | high (binary framing, big-int XOR) |
| Reverse tunnels | `R:` built-in | implemented here |
| Multiplexing | smux | custom 5-byte header |
| Behind Traefik | yes (HTTP/WS) | yes (HTTP/WS) |
| TLS | Traefik terminates | Traefik terminates |

Pick chisel when you can ship a binary to the client; pick the Python version
when the client is locked to Python 3.6 with no package install.

## Alternative: frp

`frp` (`fatedier/frp`) is another Go option with the same model. Its client can
connect to `frps` over `wss` so it also sits behind Traefik:

```toml
# frpc.toml (client)
serverAddr = "tunnel.example.com"
serverPort = 443
transport.protocol = "wss"
transport.tls.enable = true
auth.token = "S3CR3T"

[[proxies]]
name = "ssh"
type = "tcp"
localIP = "127.0.0.1"
localPort = 22
remotePort = 6767
```

With `proxyBindAddr = "127.0.0.1"` on the server, `remotePort 6767` listens on
loopback only, satisfying the "only 80/443 public" rule.
