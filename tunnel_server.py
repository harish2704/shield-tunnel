#!/usr/bin/env python3
"""
tunnel_server.py - Server side of the ssh -R equivalent tunnel.

Run this on the REMOTE machine, behind Traefik. It speaks plain WebSocket
(Traefik terminates TLS). For each REGISTER the client sends, it starts a TCP
listener on (bind_host, remotePort). When a connection arrives there, it asks
the client (over the single multiplexed WebSocket) to connect to the client's
local target and relays bytes both ways.

Usage:
    python3 tunnel_server.py --listen 127.0.0.1:8000 --bind 127.0.0.1 --secret S3CR3T

Only the Python standard library is used.
"""

import argparse
import asyncio
import sys
import time

from wsutil import (
    WSConn,
    server_handshake,
    CMD_OPEN,
    CMD_READY,
    CMD_OPEN_FAIL,
    CMD_DATA,
    CMD_CLOSE,
    CMD_REGISTER,
    CMD_ERROR,
)


def log(*a):
    print(time.strftime("%H:%M:%S"), "[server]", *a, file=sys.stderr, flush=True)


def parse_mapping(s):
    """'6767:127.0.0.1:22' -> (6767, '127.0.0.1', 22). Handles IPv6 targets."""
    rp, _, rest = s.partition(":")
    th, _, tp = rest.rpartition(":")
    return int(rp), th, int(tp)


class Session:
    def __init__(self, ws, bind_host):
        self.ws = ws
        self.bind_host = bind_host
        self.streams = {}      # sid -> (reader, writer)   active, after READY
        self.pending = {}      # sid -> (reader, writer)   waiting for client READY
        self.listeners = {}    # remote_port -> asyncio.Server
        self.next_id = 1

    def alloc_id(self):
        sid = self.next_id
        self.next_id = (self.next_id + 1) & 0xFFFFFFFF
        if self.next_id == 0:
            self.next_id = 1
        return sid

    async def handle_inbound(self, tcp_r, tcp_w, remote_port, target):
        sid = self.alloc_id()
        self.pending[sid] = (tcp_r, tcp_w)
        try:
            await self.ws.send_message(CMD_OPEN, sid, target.encode("utf-8"))
        except Exception:
            await self._drop(sid)

    async def register(self, mapping_str):
        try:
            rp, th, tp = parse_mapping(mapping_str)
        except Exception:
            await self.ws.send_message(CMD_ERROR, 0, ("bad mapping: %s" % mapping_str).encode())
            return
        if rp in self.listeners:
            log("already listening on remotePort", rp)
            return
        target = "%s:%d" % (th, tp)
        try:
            srv = await asyncio.start_server(
                lambda r, w, t=target: self.handle_inbound(r, w, rp, t),
                self.bind_host, rp,
            )
        except OSError as e:
            log("bind remotePort %d failed: %s" % (rp, e))
            await self.ws.send_message(CMD_ERROR, 0, ("bind %d failed: %s" % (rp, e)).encode())
            return
        self.listeners[rp] = srv
        log("remotePort %d -> client target %s (listening on %s:%d)"
            % (rp, target, self.bind_host, rp))

    async def run(self):
        try:
            while True:
                cmd, sid, payload = await self.ws.recv_message()
                if cmd == CMD_REGISTER:
                    for m in payload.decode("utf-8", "replace").split("\n"):
                        m = m.strip()
                        if m:
                            await self.register(m)
                elif cmd == CMD_READY:
                    ent = self.pending.pop(sid, None)
                    if ent is None:
                        continue
                    self.streams[sid] = ent
                    asyncio.ensure_future(self.pipe_tcp_to_ws(sid, ent[0]))
                elif cmd == CMD_OPEN_FAIL:
                    await self._drop(sid)
                elif cmd == CMD_DATA:
                    ent = self.streams.get(sid)
                    if ent is not None:
                        try:
                            ent[1].write(payload)
                            await ent[1].drain()
                        except Exception:
                            await self.close_stream(sid)
                elif cmd == CMD_CLOSE:
                    await self.close_stream(sid)
                elif cmd == CMD_ERROR:
                    log("client error:", payload.decode("utf-8", "replace"))
        except (ConnectionError, asyncio.IncompleteReadError, EOFError):
            pass
        except Exception as e:
            log("session loop error:", repr(e))
        finally:
            await self.cleanup()

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
        ent = self.streams.pop(sid, None) or self.pending.pop(sid, None)
        if ent is not None:
            try:
                ent[1].close()
            except Exception:
                pass

    async def _drop(self, sid):
        ent = self.pending.pop(sid, None) or self.streams.pop(sid, None)
        if ent is not None:
            try:
                ent[1].close()
            except Exception:
                pass

    async def cleanup(self):
        for ent in list(self.streams.values()) + list(self.pending.values()):
            try:
                ent[1].close()
            except Exception:
                pass
        self.streams.clear()
        self.pending.clear()
        for srv in self.listeners.values():
            try:
                srv.close()
            except Exception:
                pass
        self.listeners.clear()
        try:
            await self.ws.aclose()
        except Exception:
            pass


async def server_main(args):
    async def handle_conn(reader, writer):
        peer = writer.get_extra_info("peername")
        try:
            await server_handshake(reader, writer, args.secret)
        except Exception as e:
            log("handshake failed from %s: %s" % (peer, e))
            try:
                writer.close()
            except Exception:
                pass
            return
        ws = WSConn(reader, writer, is_client=False)
        log("tunnel client connected from %s" % (peer,))
        sess = Session(ws, args.bind)
        await sess.run()
        log("tunnel client disconnected")

    srv = await asyncio.start_server(
        handle_conn, args.listen_host, args.listen_port
    )
    log("WS tunnel server on %s:%d, exposing remotePorts on %s, secret=%s"
        % (args.listen_host, args.listen_port, args.bind,
           "yes" if args.secret else "NO (open!)"))
    log("Put Traefik in front: route Host/path -> http://%s:%d"
        % (args.listen_host, args.listen_port))
    try:
        await asyncio.Event().wait()  # run forever
    finally:
        srv.close()


def run_main(coro):
    if hasattr(asyncio, "run"):
        asyncio.run(coro)
    else:  # Python 3.6
        loop = asyncio.get_event_loop()
        loop.run_until_complete(coro)


def main():
    ap = argparse.ArgumentParser(description="ssh -R equivalent tunnel server (behind Traefik)")
    ap.add_argument("--listen", default="127.0.0.1:8000",
                    help="address to listen for WebSocket (Traefik forwards here). Default 127.0.0.1:8000")
    ap.add_argument("--bind", default="127.0.0.1",
                    help="interface for exposed remotePorts. Default 127.0.0.1 (loopback only). "
                         "Set to 0.0.0.0 to expose publicly (NOT recommended; use Traefik instead).")
    ap.add_argument("--secret", default=None, help="shared secret required from clients")
    args = ap.parse_args()
    host, _, port = args.listen.partition(":")
    args.listen_host = host
    args.listen_port = int(port)
    run_main(server_main(args))


if __name__ == "__main__":
    main()
