package wsutil

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

// WebSocket opcodes from RFC 6455.
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// MaxPayload caps a single WebSocket message size to avoid unbounded allocation.
const MaxPayload = 16 << 20 // 16 MiB

// ErrFrameTooLarge is returned when a frame's payload exceeds MaxPayload.
var ErrFrameTooLarge = errors.New("wsutil: frame too large")

// xorMaskInPlace XORs b with the repeating 4-byte key (WebSocket masking).
func xorMaskInPlace(b, key []byte) {
	if len(key) == 0 {
		return
	}
	for i := range b {
		b[i] ^= key[i&3]
	}
}

// WriteFrame writes one FIN WebSocket frame to w. If mask is true the payload is
// masked with a random 4-byte key (required for client->server frames).
func WriteFrame(w io.Writer, opcode byte, payload []byte, mask bool) error {
	var hdr [14]byte // 1 (b0) + 1 (b1) + 8 (len64) + 4 (mask) = max 14
	n := 0
	hdr[0] = 0x80 | opcode // FIN=1
	n++

	var maskKey [4]byte
	if mask {
		if _, err := readRand(maskKey[:]); err != nil {
			return err
		}
	}
	maskFlag := byte(0)
	if mask {
		maskFlag = 0x80
	}
	plen := len(payload)
	if plen < 126 {
		hdr[n] = maskFlag | byte(plen)
		n++
	} else if plen < 0x10000 {
		hdr[n] = maskFlag | 126
		n++
		binary.BigEndian.PutUint16(hdr[n:n+2], uint16(plen))
		n += 2
	} else {
		hdr[n] = maskFlag | 127
		n++
		binary.BigEndian.PutUint64(hdr[n:n+8], uint64(plen))
		n += 8
	}
	if mask {
		copy(hdr[n:n+4], maskKey[:])
		n += 4
	}

	if _, err := w.Write(hdr[:n]); err != nil {
		return err
	}
	if plen == 0 {
		return nil
	}
	if mask {
		masked := make([]byte, plen)
		copy(masked, payload)
		xorMaskInPlace(masked, maskKey[:])
		_, err := w.Write(masked)
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ReadFrame reads one logical WebSocket message from r, handling masking, the
// 126/127 extended-length forms, and continuation frames. Control frames
// (close/ping/pong) are returned immediately with their opcode.
func ReadFrame(r *bufio.Reader) (byte, []byte, error) {
	var chunks [][]byte
	var firstOp byte
	for {
		b0, err := r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		fin := b0 & 0x80
		op := b0 & 0x0F

		b1, err := r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		masked := b1 & 0x80
		length := int(b1 & 0x7F)
		if length == 126 {
			var l [2]byte
			if _, err := io.ReadFull(r, l[:]); err != nil {
				return 0, nil, err
			}
			length = int(binary.BigEndian.Uint16(l[:]))
		} else if length == 127 {
			var l [8]byte
			if _, err := io.ReadFull(r, l[:]); err != nil {
				return 0, nil, err
			}
			length = int(binary.BigEndian.Uint64(l[:]))
		}
		if length > MaxPayload {
			return 0, nil, ErrFrameTooLarge
		}

		var maskKey [4]byte
		if masked != 0 {
			if _, err := io.ReadFull(r, maskKey[:]); err != nil {
				return 0, nil, err
			}
		}
		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(r, payload); err != nil {
				return 0, nil, err
			}
		}
		if masked != 0 {
			xorMaskInPlace(payload, maskKey[:])
		}

		if op == opClose || op == opPing || op == opPong {
			return op, payload, nil // control frames are never fragmented
		}
		if firstOp == 0 {
			firstOp = op
		}
		chunks = append(chunks, payload)
		if fin != 0 {
			if len(chunks) == 1 {
				return firstOp, chunks[0], nil
			}
			return firstOp, concat(chunks), nil
		}
	}
}

func concat(chunks [][]byte) []byte {
	n := 0
	for _, c := range chunks {
		n += len(c)
	}
	out := make([]byte, n)
	off := 0
	for _, c := range chunks {
		copy(out[off:], c)
		off += len(c)
	}
	return out
}
