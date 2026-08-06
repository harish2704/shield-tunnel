// Command tunnel-client runs the ssh -R equivalent tunnel client (Python 3.6
// constrained use case is handled by the Python client; this is the Go client).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"tunnel/tunnel"
)

// reverseFlag is a repeatable -R flag (no default).
type reverseFlag []string

func (r *reverseFlag) String() string     { return fmt.Sprint(*r) }
func (r *reverseFlag) Set(v string) error { *r = append(*r, v); return nil }

func main() {
	var rev reverseFlag
	flag.Var(&rev, "R", `reverse mapping "remotePort:targetHost:targetPort" (repeatable, e.g. -R 6767:127.0.0.1:22)`)
	secret := flag.String("secret", "", "shared secret sent as X-Tunnel-Secret")
	insecure := flag.Bool("insecure", false, "skip TLS cert verification (self-signed)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: tunnel-client [flags] <wss://host[:port]/path|ws://host:port/path>")
		os.Exit(2)
	}
	url := args[0]

	if len(rev) == 0 {
		fmt.Fprintln(os.Stderr, "tunnel-client: at least one -R mapping is required")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := tunnel.RunClient(ctx, tunnel.ClientConfig{
		URL:      url,
		Secret:   *secret,
		Reverse:  rev,
		Insecure: *insecure,
	}); err != nil {
		os.Stderr.WriteString("tunnel-client: " + err.Error() + "\n")
		os.Exit(1)
	}
}
