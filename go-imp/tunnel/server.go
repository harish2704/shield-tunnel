// Package tunnel implements the ssh -R equivalent tunnel server and client,
// mirroring the Python implementation. It uses the wsutil package for the
// dependency-free WebSocket transport + multiplexing, so a Go client/server is
// wire-compatible with the Python client/server.
package tunnel

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"tunnel/wsutil"
)

// ServerConfig configures the tunnel server.
type ServerConfig struct {
	ListenAddr string // e.g. "127.0.0.1:8000" (Traefik forwards 443 here)
	BindHost   string // interface for exposed remotePorts, e.g. "127.0.0.1"
	Secret     string // shared secret; empty = no auth
}

// Server is a tunnel server. Use NewServer to create one, Serve to run.
type Server struct {
	cfg ServerConfig
	ln  net.Listener
}

func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.BindHost == "" {
		cfg.BindHost = "127.0.0.1"
	}
	host, port, err := net.SplitHostPort(cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("tunnel: bad listen addr: %w", err)
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, err
	}
	return &Server{cfg: cfg, ln: ln}, nil
}

// Addr returns the listener address.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts WebSocket connections until ctx is done or a fatal error.
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		_ = s.ln.Close()
	}()
	log.Printf("[server] WS tunnel server on %s, exposing remotePorts on %s, secret=%t",
		s.cfg.ListenAddr, s.cfg.BindHost, s.cfg.Secret != "")
	log.Printf("[server] Put Traefik in front: route Host/path -> http://%s", s.cfg.ListenAddr)

	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	peer := conn.RemoteAddr().String()
	ws := wsutil.NewWSConn(conn, false) // server -> frames unmasked
	if _, err := ws.ServerHandshake(s.cfg.Secret); err != nil {
		log.Printf("[server] handshake failed from %s: %s", peer, err)
		_ = conn.Close()
		return
	}
	log.Printf("[server] tunnel client connected from %s", peer)
	sess := &serverSession{ws: ws, bindHost: s.cfg.BindHost, streams: map[uint32]net.Conn{}, pending: map[uint32]net.Conn{}, listeners: map[int]net.Listener{}}
	sess.run(ctx)
}

// serverSession holds one client's multiplexed connection state.
type serverSession struct {
	ws        *wsutil.WSConn
	bindHost  string
	mu        sync.Mutex // protects maps + nextID
	streams   map[uint32]net.Conn
	pending   map[uint32]net.Conn
	listeners map[int]net.Listener
	nextID    uint32
}

func (s *serverSession) allocID() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextID
	s.nextID++
	if s.nextID == 0 {
		s.nextID = 1
	}
	return id
}

func (s *serverSession) run(ctx context.Context) {
	// Close the WS if the context is cancelled to unblock RecvMessage.
	go func() {
		<-ctx.Done()
		_ = s.ws.Close()
	}()
	for {
		cmd, sid, payload, err := s.ws.RecvMessage()
		if err != nil {
			break
		}
		switch cmd {
		case wsutil.CmdRegister:
			for _, m := range strings.Split(string(payload), "\n") {
				m = strings.TrimSpace(m)
				if m != "" {
					s.register(m)
				}
			}
		case wsutil.CmdReady:
			s.mu.Lock()
			c := s.pending[sid]
			delete(s.pending, sid)
			if c != nil {
				s.streams[sid] = c
			}
			s.mu.Unlock()
			if c != nil {
				go s.pipeTCPToWS(sid, c)
			}
		case wsutil.CmdOpenFail:
			s.mu.Lock()
			c := s.pending[sid]
			delete(s.pending, sid)
			s.mu.Unlock()
			if c != nil {
				_ = c.Close()
			}
		case wsutil.CmdData:
			s.mu.Lock()
			c := s.streams[sid]
			s.mu.Unlock()
			if c != nil {
				_, _ = c.Write(payload)
			}
		case wsutil.CmdClose:
			s.closeStream(sid)
		case wsutil.CmdError:
			log.Printf("[server] client error: %s", string(payload))
		}
	}
	s.cleanup()
}

func (s *serverSession) register(mapping string) {
	rp, host, port, ok := parseMapping(mapping)
	if !ok {
		_ = s.ws.SendMessage(wsutil.CmdError, 0, []byte("bad mapping: "+mapping))
		return
	}
	s.mu.Lock()
	if _, exists := s.listeners[rp]; exists {
		s.mu.Unlock()
		return // already listening
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.bindHost, rp))
	if err != nil {
		s.mu.Unlock()
		log.Printf("[server] bind remotePort %d failed: %s", rp, err)
		_ = s.ws.SendMessage(wsutil.CmdError, 0, []byte(fmt.Sprintf("bind %d failed: %s", rp, err)))
		return
	}
	s.listeners[rp] = ln
	s.mu.Unlock()

	target := fmt.Sprintf("%s:%d", host, port)
	log.Printf("[server] remotePort %d -> client target %s (listening on %s:%d)", rp, target, s.bindHost, rp)

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.handleInbound(c, target)
		}
	}()
}

func (s *serverSession) handleInbound(c net.Conn, target string) {
	sid := s.allocID()
	s.mu.Lock()
	s.pending[sid] = c
	s.mu.Unlock()
	_ = s.ws.SendMessage(wsutil.CmdOpen, sid, []byte(target))
}

func (s *serverSession) pipeTCPToWS(sid uint32, c net.Conn) {
	buf := make([]byte, 65536)
	for {
		n, err := c.Read(buf)
		if n > 0 {
			if e := s.ws.SendMessage(wsutil.CmdData, sid, buf[:n]); e != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	_ = s.ws.SendMessage(wsutil.CmdClose, sid, nil)
	s.closeStream(sid)
}

func (s *serverSession) closeStream(sid uint32) {
	s.mu.Lock()
	c := s.streams[sid]
	delete(s.streams, sid)
	if c == nil {
		c = s.pending[sid]
		delete(s.pending, sid)
	}
	s.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

func (s *serverSession) cleanup() {
	s.mu.Lock()
	for _, c := range s.streams {
		_ = c.Close()
	}
	for _, c := range s.pending {
		_ = c.Close()
	}
	s.streams = nil
	s.pending = nil
	for _, ln := range s.listeners {
		_ = ln.Close()
	}
	s.listeners = nil
	s.mu.Unlock()
	_ = s.ws.Close()
	log.Printf("[server] tunnel client disconnected")
}

// parseMapping parses "6767:127.0.0.1:22" -> (6767, "127.0.0.1", 22, true).
// Mirrors Python: first ':' splits remotePort, last ':' in the rest splits host/port.
func parseMapping(s string) (rp int, host string, port int, ok bool) {
	i := strings.Index(s, ":")
	if i < 0 {
		return 0, "", 0, false
	}
	rest := s[i+1:]
	j := strings.LastIndex(rest, ":")
	if j < 0 {
		return 0, "", 0, false
	}
	rpStr := strings.TrimSpace(s[:i])
	hostStr := strings.TrimSpace(rest[:j])
	portStr := strings.TrimSpace(rest[j+1:])
	var err error
	if rp, err = strconv.Atoi(rpStr); err != nil {
		return 0, "", 0, false
	}
	if port, err = strconv.Atoi(portStr); err != nil {
		return 0, "", 0, false
	}
	return rp, hostStr, port, true
}
