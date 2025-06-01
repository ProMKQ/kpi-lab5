package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/ProMKQ/kpi-lab5/datastore"
)

var db *datastore.Db

func main() {
	var err error
	db, err = datastore.OpenWithSegmentLimit("db-data", 1024)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	http.HandleFunc("/db/", handleDB)
	http.HandleFunc("/api/v1/some-data", handleSomeData)

	port := ":8081"
	fmt.Println("DB service listening on", port)
	log.Fatal(http.ListenAndServe("0.0.0.0"+port, nil))
}

func handleDB(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		typ := r.URL.Query().Get("type")
		if typ == "" {
			typ = "string"
		}

		switch typ {
		case "string":
			val, err := db.Get(key)
			if err != nil {
				http.Error(w, "", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"key":   key,
				"value": val,
			})

		case "int64":
			val, err := db.GetInt64(key)
			if err != nil {
				http.Error(w, "", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"key":   key,
				"value": val,
			})

		default:
			http.Error(w, "unsupported type", http.StatusBadRequest)
		}

	case http.MethodPost:
		var data map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		rawVal, ok := data["value"]
		if !ok {
			http.Error(w, "missing value", http.StatusBadRequest)
			return
		}

		switch v := rawVal.(type) {
		case string:
			err := db.Put(key, v)
			if err != nil {
				http.Error(w, "put error", http.StatusInternalServerError)
			}
		case float64:
			err := db.PutInt64(key, int64(v))
			if err != nil {
				http.Error(w, "put error", http.StatusInternalServerError)
			}
		default:
			http.Error(w, "invalid value type", http.StatusBadRequest)
		}

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleSomeData(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	typ := r.URL.Query().Get("type")
	if typ == "" {
		typ = "string"
	}

	switch typ {
	case "string":
		value, err := db.Get(key)
		if err != nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": value,
		})

	case "int64":
		value, err := db.GetInt64(key)
		if err != nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"key":   key,
			"value": value,
		})

	default:
		http.Error(w, "unsupported type", http.StatusBadRequest)
	}
}
