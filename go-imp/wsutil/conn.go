package wsutil

import (
	"bufio"
	"errors"
	"net"
	"sync"
)

// ErrClosed is returned by RecvMessage when the peer closes the WebSocket.
var ErrClosed = errors.New("websocket closed by peer")

// WSConn wraps an upgraded (or about-to-be-upgraded) net.Conn as a multiplexed
// message pipe. Writes are serialized by a mutex; the same buffered reader used
// during the handshake is reused afterwards so any over-read bytes are kept.
type WSConn struct {
	conn     net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	isClient bool // true => outbound frames must be masked
	mu       sync.Mutex
}

// NewWSConn wraps conn. isClient controls whether sent frames are masked
// (per RFC 6455, only client->server frames are masked).
func NewWSConn(conn net.Conn, isClient bool) *WSConn {
	return &WSConn{
		conn:     conn,
		r:        bufio.NewReaderSize(conn, 65536),
		w:        bufio.NewWriterSize(conn, 65536),
		isClient: isClient,
	}
}

// SendMessage encodes and sends one application message as a binary frame.
func (ws *WSConn) SendMessage(cmd byte, sid uint32, payload []byte) error {
	msg := EncodeMessage(cmd, sid, payload)
	return ws.writeFrame(opBinary, msg)
}

// SendPing sends a WebSocket-level PING (used for keepalive).
func (ws *WSConn) SendPing(payload []byte) error {
	return ws.writeFrame(opPing, payload)
}

func (ws *WSConn) writeFrame(opcode byte, data []byte) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if err := WriteFrame(ws.w, opcode, data, ws.isClient); err != nil {
		return err
	}
	return ws.w.Flush()
}

// RecvMessage returns the next application data message. Pings are answered
// with pongs transparently; a peer close returns ErrClosed.
func (ws *WSConn) RecvMessage() (byte, uint32, []byte, error) {
	for {
		op, data, err := ReadFrame(ws.r)
		if err != nil {
			return 0, 0, nil, err
		}
		switch op {
		case opClose:
			_ = ws.writeFrame(opClose, nil) // best-effort echo
			return 0, 0, nil, ErrClosed
		case opPing:
			_ = ws.writeFrame(opPong, data)
			continue
		case opPong:
			continue
		case opText, opBinary:
			if len(data) < 1 {
				continue
			}
			m, ok := DecodeMessage(data)
			if !ok {
				continue
			}
			return m.Cmd, m.StreamID, m.Payload, nil
		}
	}
}

// Close sends a close frame and closes the underlying connection.
func (ws *WSConn) Close() error {
	_ = ws.writeFrame(opClose, nil)
	return ws.conn.Close()
}

// Underlying returns the underlying net.Conn (e.g. to close it to break a recv).
func (ws *WSConn) Underlying() net.Conn { return ws.conn }
