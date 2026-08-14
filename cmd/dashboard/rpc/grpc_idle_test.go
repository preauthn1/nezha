package rpc

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/bufconn"
)

type connEndStats struct{ ended chan struct{} }

func (h *connEndStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (h *connEndStats) HandleRPC(context.Context, stats.RPCStats)                       {}
func (h *connEndStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h *connEndStats) HandleConn(_ context.Context, event stats.ConnStats) {
	if _, ok := event.(*stats.ConnEnd); ok {
		select {
		case <-h.ended:
		default:
			close(h.ended)
		}
	}
}

func TestGRPCServerClosesIdleTransport(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	observer := &connEndStats{ended: make(chan struct{})}
	server := grpc.NewServer(append(grpcServerOptions(50*time.Millisecond), grpc.StatsHandler(observer))...)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if conn.GetState() != connectivity.Ready {
		t.Fatalf("connection state=%s, want READY", conn.GetState())
	}

	select {
	case <-observer.ended:
	case <-ctx.Done():
		t.Fatal("idle gRPC transport was not closed")
	}
}
