package tunnel

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"tunnel/wsutil"
)

// ClientConfig configures the tunnel client.
type ClientConfig struct {
	URL      string // wss://host[:port]/path or ws://host:port/path
	Secret   string
	Reverse  []string // "remotePort:targetHost:targetPort"
	Insecure bool     // skip TLS cert verification (self-signed)
}

// RunClient connects outbound to the server (through Traefik), registers its
// reverse mappings, and stays connected. It reconnects with exponential
// backoff until ctx is done.
func RunClient(ctx context.Context, cfg ClientConfig) error {
	if len(cfg.Reverse) == 0 {
		return fmt.Errorf("tunnel: no reverse mappings given")
	}
	backoff := time.Second
	for {
		err := runClientSession(ctx, cfg)
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("[client] disconnected: %v", err)
		log.Printf("[client] reconnecting in %v ...", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 60*time.Second {
			backoff = 60 * time.Second
		}
	}
}

func runClientSession(ctx context.Context, cfg ClientConfig) error {
	scheme, host, port, path, err := parseURL(cfg.URL)
	if err != nil {
		return err
	}
	log.Printf("[client] connecting to %s ...", cfg.URL)

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	var conn net.Conn
	if scheme == "wss" {
		tlsConf := &tls.Config{ServerName: host}
		if cfg.Insecure {
			tlsConf.InsecureSkipVerify = true
		}
		conn, err = tls.Dial("tcp", addr, tlsConf)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return err
	}

	ws := wsutil.NewWSConn(conn, true) // client -> frames masked
	if err := ws.ClientHandshake(host, port, path, cfg.Secret); err != nil {
		_ = conn.Close()
		return err
	}
	log.Printf("[client] connected (TLS terminated by Traefik)")

	// Register all reverse mappings in one REGISTER message (newline-joined).
	reg := strings.Join(cfg.Reverse, "\n")
	if err := ws.SendMessage(wsutil.CmdRegister, 0, []byte(reg)); err != nil {
		return err
	}
	log.Printf("[client] registered %d mapping(s): %s", len(cfg.Reverse), strings.Join(cfg.Reverse, ", "))

	// Keepalive via WebSocket PING every 30s.
	stopKeep := make(chan struct{})
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopKeep:
				return
			case <-t.C:
				_ = ws.SendPing([]byte("k"))
			}
		}
	}()
	// Cancel the WS recv if the context is done.
	go func() {
		<-ctx.Done()
		_ = ws.Close()
	}()

	var smu sync.Mutex
	streams := map[uint32]net.Conn{}

	closeStream := func(sid uint32) {
		smu.Lock()
		c := streams[sid]
		delete(streams, sid)
		smu.Unlock()
		if c != nil {
			_ = c.Close()
		}
	}

	for {
		cmd, sid, payload, err := ws.RecvMessage()
		if err != nil {
			close(stopKeep)
			// tear down any open streams
			smu.Lock()
			for _, c := range streams {
				_ = c.Close()
			}
			smu.Unlock()
			return err
		}
		switch cmd {
		case wsutil.CmdOpen:
			go openStream(ws, sid, string(payload), &smu, streams)
		case wsutil.CmdData:
			smu.Lock()
			c := streams[sid]
			smu.Unlock()
			if c != nil {
				_, _ = c.Write(payload)
			} else {
				_ = ws.SendMessage(wsutil.CmdClose, sid, nil)
			}
		case wsutil.CmdClose:
			closeStream(sid)
		case wsutil.CmdError:
			log.Printf("[client] server error: %s", string(payload))
		}
	}
}

func openStream(ws *wsutil.WSConn, sid uint32, target string, mu *sync.Mutex, streams map[uint32]net.Conn) {
	c, err := net.Dial("tcp", target)
	if err != nil {
		log.Printf("[client] connect to local target %s failed: %s", target, err)
		_ = ws.SendMessage(wsutil.CmdOpenFail, sid, nil)
		return
	}
	mu.Lock()
	streams[sid] = c
	mu.Unlock()
	if err := ws.SendMessage(wsutil.CmdReady, sid, nil); err != nil {
		mu.Lock()
		delete(streams, sid)
		mu.Unlock()
		_ = c.Close()
		return
	}
	buf := make([]byte, 65536)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			if e := ws.SendMessage(wsutil.CmdData, sid, buf[:n]); e != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = ws.SendMessage(wsutil.CmdClose, sid, nil)
	mu.Lock()
	delete(streams, sid)
	mu.Unlock()
	_ = c.Close()
}

// parseURL parses "ws://host:port/path" / "wss://host:port/path".
func parseURL(raw string) (scheme, host string, port int, path string, err error) {
	if strings.HasPrefix(raw, "wss://") {
		return splitHP("wss", raw[len("wss://"):])
	}
	if strings.HasPrefix(raw, "ws://") {
		return splitHP("ws", raw[len("ws://"):])
	}
	return "", "", 0, "", fmt.Errorf("tunnel: url must start with ws:// or wss://")
}

func splitHP(scheme, rest string) (scheme_, host string, port int, path string, err error) {
	hostport := rest
	if i := strings.Index(rest, "/"); i >= 0 {
		hostport = rest[:i]
		path = rest[i:]
	} else {
		path = "/"
	}
	if h, p, e := net.SplitHostPort(hostport); e == nil {
		host = h
		port, err = strconv.Atoi(p)
		if err != nil {
			return
		}
	} else {
		host = hostport
		port = 443
		if scheme == "ws" {
			port = 80
		}
	}
	scheme_ = scheme
	return
}
