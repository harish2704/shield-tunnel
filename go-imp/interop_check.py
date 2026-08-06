#!/usr/bin/env python3
"""Interop probe: an echo target + a verifier that connects to the exposed
remote port and checks small echo, 1 MB, and 10 concurrent streams.

Usage: interop_check.py <echo_port> <remote_port>

The tunnel server + client (one Go, one Python) are started separately by the
caller. This script only provides the echo target and verifies the tunnel.
"""
import asyncio, os, sys, time

async def echo(reader, writer):
    try:
        while True:
            d = await reader.read(65536)
            if not d: break
            writer.write(d); await writer.drain()
    except Exception:
        pass
    finally:
        try: writer.close()
        except: pass

async def wait_port(port, timeout=10):
    dl = time.time()+timeout
    while time.time() < dl:
        try:
            r,w = await asyncio.open_connection("127.0.0.1", port); w.close(); return True
        except Exception:
            await asyncio.sleep(0.05)
    return False

async def main():
    echo_port = int(sys.argv[1]); remote_port = int(sys.argv[2])
    await asyncio.start_server(echo, "127.0.0.1", echo_port)
    if not await wait_port(remote_port, 15):
        print("FAIL: remote port %d not opened" % remote_port); sys.exit(1)
    # small
    r,w = await asyncio.open_connection("127.0.0.1", remote_port)
    w.write(b"hello\n"); await w.drain()
    got = await r.readexactly(6); w.close()
    assert got == b"hello\n", got
    # 1 MB
    payload = os.urandom(1000000)
    r,w = await asyncio.open_connection("127.0.0.1", remote_port)
    w.write(payload); await w.drain()
    buf = b""
    while len(buf) < len(payload):
        c = await r.read(65536)
        if not c: break
        buf += c
    w.close(); assert buf == payload, "1MB %d vs %d" % (len(buf), len(payload))
    # 10 concurrent
    async def one(i):
        r,w = await asyncio.open_connection("127.0.0.1", remote_port)
        msg = ("s%d-" % i).encode() + os.urandom(500)
        w.write(msg); await w.drain()
        b = b""
        while len(b) < len(msg):
            c = await r.read(65536)
            if not c: break
            b += c
        w.close(); assert b == msg, i
    await asyncio.gather(*[one(i) for i in range(10)])
    print("INTEROP OK (small + 1MB + 10 streams)")

asyncio.run(main())
