package tunnel

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// End-to-end loopback test: Go server + Go client + an echo target, all in
// process, over plain ws:// (no TLS, no Traefik). Mirrors test_loopback.py.

const (
	testEchoPort   = 2200  // stands in for the client's local SSH (22)
	testRemotePort = 16767 // exposed on the "remote" side
	testWSPort     = 18000
	testSecret     = "s3cr3t"
)

func startEchoServer(t *testing.T, port int) net.Listener {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c) // echo
			}(c)
		}
	}()
	return ln
}

func waitPort(t *testing.T, port int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("port %d never opened", port)
}

func dialRemote(t *testing.T) net.Conn {
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", testRemotePort))
	if err != nil {
		t.Fatalf("dial remote: %v", err)
	}
	return c
}

func TestLoopback(t *testing.T) {
	echo := startEchoServer(t, testEchoPort)
	defer echo.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := NewServer(ServerConfig{
		ListenAddr: fmt.Sprintf("127.0.0.1:%d", testWSPort),
		BindHost:   "127.0.0.1",
		Secret:     testSecret,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	go func() { _ = srv.Serve(ctx) }()

	go func() {
		_ = RunClient(ctx, ClientConfig{
			URL:     fmt.Sprintf("ws://127.0.0.1:%d/tunnel", testWSPort),
			Secret:  testSecret,
			Reverse: []string{fmt.Sprintf("%d:127.0.0.1:%d", testRemotePort, testEchoPort)},
		})
	}()

	waitPort(t, testRemotePort, 10*time.Second)

	// 1) small echo
	c := dialRemote(t)
	c.Write([]byte("hello\n"))
	buf := make([]byte, 6)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("small echo read: %v", err)
	}
	if !bytes.Equal(buf, []byte("hello\n")) {
		t.Fatalf("small echo mismatch: %q", buf)
	}
	c.Close()

	// 2) 1 MB random transfer (exercises 8-byte WS length form + masking)
	payload := make([]byte, 1000000)
	_, _ = crand.Read(payload)
	c = dialRemote(t)
	go func() {
		_, _ = c.Write(payload)
	}()
	got := make([]byte, 0, len(payload))
	rbuf := make([]byte, 65536)
	for len(got) < len(payload) {
		n, err := c.Read(rbuf)
		if n > 0 {
			got = append(got, rbuf[:n]...)
		}
		if err != nil {
			break
		}
	}
	c.Close()
	if !bytes.Equal(got, payload) {
		t.Fatalf("1MB mismatch: %d vs %d bytes", len(got), len(payload))
	}

	// 3) 10 concurrent multiplexed streams
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := dialRemote(t)
			defer c.Close()
			msg := append([]byte(fmt.Sprintf("stream-%d-", i)), make([]byte, 500)...)
			_, _ = c.Write(msg)
			acc := make([]byte, 0, len(msg))
			tmp := make([]byte, 65536)
			for len(acc) < len(msg) {
				n, err := c.Read(tmp)
				if n > 0 {
					acc = append(acc, tmp[:n]...)
				}
				if err != nil {
					break
				}
			}
			if !bytes.Equal(acc, msg) {
				t.Errorf("concurrent stream %d mismatch", i)
			}
		}(i)
	}
	wg.Wait()

	if !t.Failed() {
		t.Log("ALL GO-ONLY TESTS PASSED")
	}
}
