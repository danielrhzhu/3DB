package main

import "net/http"

func main() {
	//connect to kv engine
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		//perform read here

	})

	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		//perform write here
	})

	http.ListenAndServe(":8080", nil)
}
