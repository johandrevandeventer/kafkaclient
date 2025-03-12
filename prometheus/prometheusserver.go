package prometheusserver

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartPrometheusServer starts a Prometheus metrics server.
func StartPrometheusServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(addr, nil); err != nil {
			panic(err)
		}
	}()
}
