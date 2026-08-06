#!/usr/bin/env python3
"""
tunnel_client.py - Client side of the ssh -R equivalent tunnel.

Run this on the CLIENT machine (behind NAT/firewall, Python 3.6+). It dials OUT
to Traefik over wss:// (TLS terminated by Traefik), registers one or more
reverse mappings, and stays connected. When the server receives an inbound
connection on a remotePort, it asks this client to connect to the local target
and relays bytes over the single multiplexed WebSocket.

Example (maps the client's SSH 22 onto the remote's 6767):

    python3 tunnel_client.py wss://tunnel.example.com/tunnel \
        --secret S3CR3T -R 6767:127.0.0.1:22

Multiple reverse mappings:

    -R 6767:127.0.0.1:22 -R 6768:127.0.0.1:5432

Only the Python standard library is used -> works on a bare Python 3.6 box
with no pip and no internet.
"""

import argparse
import asyncio
import ssl
import sys
import time

from wsutil import (
    WSConn,
    client_handshake,
    CMD_OPEN,
    CMD_READY,
    CMD_OPEN_FAIL,
    CMD_DATA,
    CMD_CLOSE,
    CMD_REGISTER,
    CMD_ERROR,
)


def log(*a):
    print(time.strftime("%H:%M:%S"), "[client]", *a, file=sys.stderr, flush=True)


def parse_url(url):
    if url.startswith("wss://"):
        scheme = "wss"
        rest = url[6:]   # len("wss://") == 6
    elif url.startswith("ws://"):
        scheme = "ws"
        rest = url[5:]   # len("ws://") == 5
    else:
        raise ValueError("url must start with ws:// or wss://")
    if "/" in rest:
        hostport, path = rest.split("/", 1)
        path = "/" + path
    else:
        hostport, path = rest, "/"
    if ":" in hostport:
        host, _, port = hostport.rpartition(":")
        port = int(port)
    else:
        host = hostport
        port = 443 if scheme == "wss" else 80
    return scheme, host, port, path


async def connect_ws(url, secret, insecure):
    scheme, host, port, path = parse_url(url)
    ssl_ctx = None
    if scheme == "wss":
        ssl_ctx = ssl.create_default_context()
        if insecure:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
    kwargs = {}
    if ssl_ctx:
        kwargs["ssl"] = ssl_ctx
        kwargs["server_hostname"] = host
    reader, writer = await asyncio.open_connection(host, port, **kwargs)
    await client_handshake(reader, writer, host, port, path, secret=secret)
    return WSConn(reader, writer, is_client=True)


class ClientSession:
    def __init__(self, ws, mappings):
        self.ws = ws
        self.mappings = mappings  # list of "remotePort:targetHost:targetPort"
        self.streams = {}        # sid -> (reader, writer)

    async def run(self):
        reg = "\n".join(self.mappings).encode("utf-8")
        await self.ws.send_message(CMD_REGISTER, 0, reg)
        log("registered %d mapping(s): %s" % (len(self.mappings), ", ".join(self.mappings)))
        keepalive = asyncio.ensure_future(self.keepalive())
        try:
            while True:
                cmd, sid, payload = await self.ws.recv_message()
                if cmd == CMD_OPEN:
                    target = payload.decode("utf-8", "replace")
                    asyncio.ensure_future(self.open_stream(sid, target))
                elif cmd == CMD_DATA:
                    ent = self.streams.get(sid)
                    if ent is not None:
                        try:
                            ent[1].write(payload)
                            await ent[1].drain()
                        except Exception:
                            await self.close_stream(sid)
                            try:
                                await self.ws.send_message(CMD_CLOSE, sid)
                            except Exception:
                                pass
                elif cmd == CMD_CLOSE:
                    await self.close_stream(sid)
                elif cmd == CMD_ERROR:
                    log("server error:", payload.decode("utf-8", "replace"))
        except (ConnectionError, asyncio.IncompleteReadError, EOFError):
            pass
        except Exception as e:
            log("session loop error:", repr(e))
        finally:
            keepalive.cancel()
            for _, w in list(self.streams.values()):
                try:
                    w.close()
                except Exception:
                    pass
            self.streams.clear()
            try:
                await self.ws.aclose()
            except Exception:
                pass

    async def open_stream(self, sid, target):
        th, _, tp = target.rpartition(":")
        try:
            r, w = await asyncio.open_connection(th, int(tp))
        except Exception as e:
            log("connect to local target %s failed: %s" % (target, e))
            try:
                await self.ws.send_message(CMD_OPEN_FAIL, sid)
            except Exception:
                pass
            return
        self.streams[sid] = (r, w)
        try:
            await self.ws.send_message(CMD_READY, sid)
        except Exception:
            await self.close_stream(sid)
            return
        asyncio.ensure_future(self.pipe_tcp_to_ws(sid, r))

    async def pipe_tcp_to_ws(self, sid, tcp_r):
        try:
            while True:
                data = await tcp_r.read(65536)
                if not data:
                    break
                await self.ws.send_message(CMD_DATA, sid, data)
        except Exception:
            pass
        finally:
            try:
                await self.ws.send_message(CMD_CLOSE, sid)
            except Exception:
                pass
            await self.close_stream(sid)

    async def close_stream(self, sid):
        ent = self.streams.pop(sid, None)
        if ent is not None:
            try:
                ent[1].close()
            except Exception:
                pass

    async def keepalive(self):
        try:
            while True:
                await asyncio.sleep(30)
                await self.ws.send_ping(b"k")
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


async def client_main(args):
    if not args.reverse:
        log("no -R mappings given; nothing to do")
        return
    backoff = 1
    while True:
        try:
            log("connecting to %s ..." % args.url)
            ws = await connect_ws(args.url, args.secret, args.insecure)
            log("connected (TLS terminated by Traefik)")
            backoff = 1
            await ClientSession(ws, list(args.reverse)).run()
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            log("disconnected: %s" % e)
        log("reconnecting in %ds ..." % backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


def run_main(coro):
    if hasattr(asyncio, "run"):
        asyncio.run(coro)
    else:  # Python 3.6
        loop = asyncio.get_event_loop()
        loop.run_until_complete(coro)


def main():
    ap = argparse.ArgumentParser(description="ssh -R equivalent tunnel client (Python 3.6+)")
    ap.add_argument("url", help="wss://host/tunnel  (through Traefik) or ws://host:port/tunnel")
    ap.add_argument("-R", "--reverse", action="append", default=[], metavar="RP:HOST:PORT",
                    help="expose local target HOST:PORT as remotePort RP. Repeatable. "
                         "e.g. -R 6767:127.0.0.1:22")
    ap.add_argument("--secret", default=None, help="shared secret sent as X-Tunnel-Secret")
    ap.add_argument("--insecure", action="store_true", help="skip TLS cert verification (self-signed)")
    args = ap.parse_args()
    try:
        run_main(client_main(args))
    except KeyboardInterrupt:
        log("bye")


if __name__ == "__main__":
    main()
