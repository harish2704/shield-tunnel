"""
wsutil.py - Minimal dependency-free WebSocket helpers + a tiny multiplexing
message layer shared by tunnel_client.py and tunnel_server.py.

Only the Python standard library is used, so the client runs on a bare
Python 3.6+ installation with NO pip installs and NO internet access.

Wire format (application layer, carried inside one WebSocket *binary* frame):

    [1 byte: command] [4 bytes: stream id, big-endian uint32] [payload bytes]

The WebSocket frame itself gives the total length, so the app layer does not
need its own length field. Binary frames only (no JSON, no base64) -> fast.

Commands:
    0x01 OPEN        server -> client : "connect to <target>", payload = "host:port"
    0x02 READY       client -> server : stream connected OK
    0x03 OPEN_FAIL   client -> server : could not connect to target
    0x04 DATA        either direction : payload = raw bytes
    0x05 CLOSE       either direction : stream ended
    0x08 REGISTER    client -> server : payload = "remotePort:targetHost:targetPort"
                                        (multiple mappings separated by '\n')
    0x09 ERROR       either direction : payload = human readable text

Keepalive uses WebSocket-level PING/PONG (opcode 0x9/0xA), handled here.
"""

import base64
import hashlib
import os
import struct

# RFC 6455
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

# App commands
CMD_OPEN = 0x01
CMD_READY = 0x02
CMD_OPEN_FAIL = 0x03
CMD_DATA = 0x04
CMD_CLOSE = 0x05
CMD_REGISTER = 0x08
CMD_ERROR = 0x09

HEADER_LEN = 5  # 1 byte cmd + 4 bytes stream id


def xor_mask(payload, key):
    """XOR `payload` with the repeating 4-byte `key` (WebSocket masking).

    Uses Python big-int arithmetic so the work happens in C instead of a slow
    per-byte Python loop. This is the hot path for masked (client->server) data.
    """
    n = len(payload)
    if not n:
        return b""
    reps = n // 4
    mask = (key * reps) + key[: n % 4]
    return (int.from_bytes(payload, "big") ^ int.from_bytes(mask, "big")).to_bytes(n, "big")


def build_frame(payload, opcode=0x2, mask=True):
    """Build a single FIN WebSocket frame (RFC 6455)."""
    out = bytearray()
    out.append(0x80 | opcode)  # FIN=1
    n = len(payload)
    mask_flag = 0x80 if mask else 0x00
    if n < 126:
        out.append(mask_flag | n)
    elif n < 0x10000:
        out.append(mask_flag | 126)
        out += struct.pack(">H", n)
    else:
        out.append(mask_flag | 127)
        out += struct.pack(">Q", n)
    if mask:
        mk = os.urandom(4)
        out += mk
        out += xor_mask(payload, mk)
    else:
        out += payload
    return bytes(out)


async def read_frame(reader):
    """Read one logical WebSocket message, returning (opcode, payload).

    Handles masking, the 126/127 extended-length forms, and continuation
    frames. Control frames (close/ping/pong) are returned immediately.
    """
    chunks = bytearray()
    first_opcode = None
    while True:
        b0 = (await reader.readexactly(1))[0]
        fin = b0 & 0x80
        opcode = b0 & 0x0F
        b1 = (await reader.readexactly(1))[0]
        masked = b1 & 0x80
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack(">H", await reader.readexactly(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", await reader.readexactly(8))[0]
        mask_key = b""
        if masked:
            mask_key = await reader.readexactly(4)
        payload = b""
        if length:
            payload = await reader.readexactly(length)
        if masked and payload:
            payload = xor_mask(payload, mask_key)
        if opcode in (0x8, 0x9, 0xA):
            return opcode, payload  # control frame, never fragmented
        if first_opcode is None:
            first_opcode = opcode if opcode != 0 else 0
        chunks += payload
        if fin:
            return first_opcode, bytes(chunks)


class WSConn:
    """Wraps an upgraded asyncio TCP connection as a multiplexed message pipe."""

    def __init__(self, reader, writer, is_client):
        self.r = reader
        self.w = writer
        self.is_client = is_client  # True => frames must be masked
        import asyncio
        self._lock = asyncio.Lock()

    async def send_message(self, cmd, sid, payload=b""):
        msg = bytes([cmd]) + struct.pack(">I", sid & 0xFFFFFFFF) + payload
        frame = build_frame(msg, opcode=0x2, mask=self.is_client)
        async with self._lock:
            self.w.write(frame)
            await self.w.drain()

    async def send_ping(self, payload=b"k"):
        frame = build_frame(payload, opcode=0x9, mask=self.is_client)
        async with self._lock:
            self.w.write(frame)
            await self.w.drain()

    async def recv_message(self):
        """Return (cmd, sid, payload) for the next DATA frame.

        Ping/pong are handled transparently; a peer close raises ConnectionError.
        """
        while True:
            opcode, data = await read_frame(self.r)
            if opcode == 0x8:  # close
                try:
                    async with self._lock:
                        self.w.write(build_frame(b"", 0x8, self.is_client))
                        await self.w.drain()
                except Exception:
                    pass
                raise ConnectionError("websocket closed by peer")
            if opcode == 0x9:  # ping -> pong
                pong = build_frame(data, opcode=0xA, mask=self.is_client)
                async with self._lock:
                    self.w.write(pong)
                    await self.w.drain()
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode in (0x1, 0x2):  # text or binary
                if len(data) < 1:
                    continue
                cmd = data[0]
                if len(data) >= HEADER_LEN:
                    sid = struct.unpack(">I", data[1:5])[0]
                    payload = data[5:]
                else:
                    sid = 0
                    payload = data[1:]
                return cmd, sid, payload
            # ignore anything else

    async def aclose(self):
        try:
            async with self._lock:
                self.w.write(build_frame(b"", 0x8, self.is_client))
                await self.w.drain()
        except Exception:
            pass
        try:
            self.w.close()
        except Exception:
            pass


async def client_handshake(reader, writer, host, port, path, secret=None, extra_headers=None):
    """Perform the WebSocket upgrade as a client. Raises on failure."""
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    host_header = host
    default_port = 443 if (port in (80, 443)) else None
    if port not in (80, 443):
        host_header = "%s:%d" % (host, port)
    lines = [
        "GET %s HTTP/1.1" % path,
        "Host: %s" % host_header,
        "Upgrade: websocket",
        "Connection: Upgrade",
        "Sec-WebSocket-Key: %s" % key,
        "Sec-WebSocket-Version: 13",
    ]
    if secret:
        lines.append("X-Tunnel-Secret: %s" % secret)
    if extra_headers:
        for k, v in extra_headers.items():
            lines.append("%s: %s" % (k, v))
    lines.append("")
    lines.append("")
    writer.write("\r\n".join(lines).encode("ascii"))
    await writer.drain()

    status = await reader.readline()
    if not status or b" 101 " not in status:
        rest = b""
        try:
            rest = await reader.read(1024)
        except Exception:
            pass
        raise ConnectionError("websocket handshake failed: %r %r" % (status, rest[:200]))

    expected = base64.b64encode(
        hashlib.sha1((key + GUID).encode("ascii")).digest()
    ).decode("ascii")
    got = None
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        try:
            k, _, v = line.decode("ascii", "replace").partition(":")
        except Exception:
            continue
        if k.strip().lower() == "sec-websocket-accept":
            got = v.strip()
    if got != expected:
        raise ConnectionError("bad Sec-WebSocket-Accept")


async def _send_http_error(writer, code, msg):
    body = ("%d %s\n" % (code, msg)).encode("ascii")
    resp = (
        "HTTP/1.1 %d %s\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n"
    ) % (code, msg, len(body))
    try:
        writer.write(resp.encode("ascii") + body)
        await writer.drain()
    except Exception:
        pass


async def server_handshake(reader, writer, secret=None):
    """Perform the WebSocket upgrade as a server. Returns the request path.

    Raises (after sending an HTTP error) if it is not a valid upgrade.
    """
    request_line = await reader.readline()
    if not request_line:
        raise ConnectionError("empty request")
    parts = request_line.decode("ascii", "replace").split()
    if len(parts) < 2 or parts[0].upper() != "GET":
        await _send_http_error(writer, 400, "Bad Request")
        raise ConnectionError("not a GET request")
    path = parts[1]

    headers = {}
    while True:
        line = await reader.readline()
        if line in (b"\r\n", b"\n", b""):
            break
        try:
            k, _, v = line.decode("ascii", "replace").partition(":")
        except Exception:
            continue
        headers[k.strip().lower()] = v.strip()

    if headers.get("upgrade", "").lower() != "websocket":
        await _send_http_error(writer, 400, "Expected WebSocket upgrade")
        raise ConnectionError("missing Upgrade: websocket")
    ws_key = headers.get("sec-websocket-key")
    if not ws_key:
        await _send_http_error(writer, 400, "Missing Sec-WebSocket-Key")
        raise ConnectionError("missing key")
    if secret is not None and headers.get("x-tunnel-secret") != secret:
        await _send_http_error(writer, 401, "Unauthorized")
        raise ConnectionError("bad tunnel secret")

    accept = base64.b64encode(
        hashlib.sha1((ws_key + GUID).encode("ascii")).digest()
    ).decode("ascii")
    resp = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: %s\r\n"
        "\r\n"
    ) % accept
    writer.write(resp.encode("ascii"))
    await writer.drain()
    return path
