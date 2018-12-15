package main

import (
	"net/http"
)

func createHealthHandler() {

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if healthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(500)
		}

	})

	http.ListenAndServe(":10080", nil)

}
