// Package wsutil implements the minimal, dependency-free WebSocket transport
// and the small multiplexing protocol shared by the Go tunnel client and
// server. It is wire-compatible with the Python implementation in ../wsutil.py.
//
// Application messages are carried one-per-WebSocket-binary-frame as:
//
//	[1 byte: command] [4 bytes: stream id, big-endian uint32] [payload bytes]
package wsutil

import "encoding/binary"

// GUID is the WebSocket magic string from RFC 6455.
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// Application commands (must match wsutil.py).
const (
	CmdOpen     = 0x01 // server -> client: connect to <target>; payload = "host:port"
	CmdReady    = 0x02 // client -> server: stream connected OK
	CmdOpenFail = 0x03 // client -> server: could not connect to target
	CmdData     = 0x04 // either direction: payload = raw bytes
	CmdClose    = 0x05 // either direction: stream ended
	CmdRegister = 0x08 // client -> server: "remotePort:targetHost:targetPort" ('\n'-separated)
	CmdError    = 0x09 // either direction: human-readable text
)

// HeaderLen is the size of the app header (cmd + uint32 stream id).
const HeaderLen = 5

// Message is a decoded application message.
type Message struct {
	Cmd      byte
	StreamID uint32
	Payload  []byte
}

// EncodeMessage builds the on-wire bytes for one app message.
// The returned slice is freshly allocated and owned by the caller.
func EncodeMessage(cmd byte, sid uint32, payload []byte) []byte {
	msg := make([]byte, HeaderLen+len(payload))
	msg[0] = cmd
	binary.BigEndian.PutUint32(msg[1:5], sid)
	copy(msg[5:], payload)
	return msg
}

// DecodeMessage decodes a WebSocket binary frame payload into a Message.
func DecodeMessage(data []byte) (Message, bool) {
	if len(data) < 1 {
		return Message{}, false
	}
	m := Message{Cmd: data[0]}
	if len(data) >= HeaderLen {
		m.StreamID = binary.BigEndian.Uint32(data[1:5])
		m.Payload = data[5:]
	} else {
		m.Payload = data[1:]
	}
	return m, true
}
