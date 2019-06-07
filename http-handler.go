package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func createHTTPHandler() {

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if healthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(500)
		}

	})
	http.Handle("/metrics", promhttp.Handler())

	http.ListenAndServe(":10080", nil)

}
