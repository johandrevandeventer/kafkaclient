package prometheusserver

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartPrometheusServer starts a Prometheus metrics server and allows it to be gracefully stopped.
func StartPrometheusServer(addr string, ctx context.Context) {
	// Create a server instance
	server := &http.Server{Addr: addr, Handler: promhttp.Handler()}

	// Run the server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	// Wait for cancellation signal (Ctrl+C, SIGINT, SIGTERM)
	<-ctx.Done()

	// Gracefully shut down the server with a timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		// If the shutdown fails, log an error (or handle accordingly)
		panic(err)
	}
}
