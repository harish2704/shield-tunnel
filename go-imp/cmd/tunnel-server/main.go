// Command tunnel-server runs the ssh -R equivalent tunnel server (behind Traefik).
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"tunnel/tunnel"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8000", "address to listen for WebSocket (Traefik forwards here)")
	bind := flag.String("bind", "127.0.0.1", "interface for exposed remotePorts (loopback only by default)")
	secret := flag.String("secret", "", "shared secret required from clients (X-Tunnel-Secret)")
	flag.Parse()

	srv, err := tunnel.NewServer(tunnel.ServerConfig{
		ListenAddr: *listen,
		BindHost:   *bind,
		Secret:     *secret,
	})
	if err != nil {
		os.Stderr.WriteString("tunnel-server: " + err.Error() + "\n")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := srv.Serve(ctx); err != nil {
		os.Stderr.WriteString("tunnel-server: " + err.Error() + "\n")
		os.Exit(1)
	}
}
