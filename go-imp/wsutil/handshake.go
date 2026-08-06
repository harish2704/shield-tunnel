package wsutil

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strings"
)

func readRand(b []byte) (int, error) { return io.ReadFull(rand.Reader, b) }

func computeAccept(key string) string {
	h := sha1.Sum([]byte(key + GUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

// ClientHandshake performs the WebSocket upgrade as a client, using ws's
// buffered reader/writer so any over-read bytes are preserved.
func (ws *WSConn) ClientHandshake(host string, port int, path, secret string) error {
	key := make([]byte, 16)
	if _, err := readRand(key); err != nil {
		return err
	}
	keyB64 := base64.StdEncoding.EncodeToString(key)

	hostHeader := host
	if port != 80 && port != 443 {
		hostHeader = fmt.Sprintf("%s:%d", host, port)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&sb, "Host: %s\r\n", hostHeader)
	sb.WriteString("Upgrade: websocket\r\n")
	sb.WriteString("Connection: Upgrade\r\n")
	fmt.Fprintf(&sb, "Sec-WebSocket-Key: %s\r\n", keyB64)
	sb.WriteString("Sec-WebSocket-Version: 13\r\n")
	if secret != "" {
		fmt.Fprintf(&sb, "X-Tunnel-Secret: %s\r\n", secret)
	}
	sb.WriteString("\r\n")

	ws.mu.Lock()
	if _, err := ws.w.WriteString(sb.String()); err != nil {
		ws.mu.Unlock()
		return err
	}
	if err := ws.w.Flush(); err != nil {
		ws.mu.Unlock()
		return err
	}
	ws.mu.Unlock()

	status, err := ws.r.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.Contains(status, " 101 ") {
		rest, _ := ws.r.ReadString('\n')
		return fmt.Errorf("wsutil: handshake failed: %q %q", strings.TrimSpace(status), strings.TrimSpace(rest))
	}

	expected := computeAccept(keyB64)
	got := ""
	for {
		line, err := ws.r.ReadString('\n')
		if err != nil {
			return err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(k), "Sec-WebSocket-Accept") {
			got = strings.TrimSpace(v)
		}
	}
	if got != expected {
		return fmt.Errorf("wsutil: bad Sec-WebSocket-Accept")
	}
	return nil
}

// ServerHandshake performs the WebSocket upgrade as a server and returns the
// request path. It sends an HTTP error and returns an error if the request is
// not a valid upgrade or the secret does not match.
func sendHTTPError(conn net.Conn, code int, msg string) {
	body := fmt.Sprintf("%d %s\n", code, msg)
	resp := fmt.Sprintf("HTTP/1.1 %d %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		code, msg, len(body), body)
	_, _ = conn.Write([]byte(resp))
}

func (ws *WSConn) ServerHandshake(secret string) (string, error) {
	requestLine, err := ws.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	requestLine = strings.TrimRight(requestLine, "\r\n")
	parts := strings.Fields(requestLine)
	if len(parts) < 2 || !strings.EqualFold(parts[0], "GET") {
		sendHTTPError(ws.conn, 400, "Bad Request")
		return "", fmt.Errorf("wsutil: not a GET request")
	}
	path := parts[1]

	headers := map[string]string{}
	for {
		line, err := ws.r.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		headers[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
	}

	if !strings.EqualFold(headers["upgrade"], "websocket") {
		sendHTTPError(ws.conn, 400, "Expected WebSocket upgrade")
		return "", fmt.Errorf("wsutil: missing Upgrade: websocket")
	}
	wsKey := headers["sec-websocket-key"]
	if wsKey == "" {
		sendHTTPError(ws.conn, 400, "Missing Sec-WebSocket-Key")
		return "", fmt.Errorf("wsutil: missing key")
	}
	if secret != "" && headers["x-tunnel-secret"] != secret {
		sendHTTPError(ws.conn, 401, "Unauthorized")
		return "", fmt.Errorf("wsutil: bad tunnel secret")
	}

	accept := computeAccept(wsKey)
	resp := fmt.Sprintf("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
	ws.mu.Lock()
	_, err = ws.w.WriteString(resp)
	if err == nil {
		err = ws.w.Flush()
	}
	ws.mu.Unlock()
	if err != nil {
		return "", err
	}
	return path, nil
}
