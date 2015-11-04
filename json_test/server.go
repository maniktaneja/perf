package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"

	"github.com/maniktaneja/perf/fetch"
	_ "net/http/pprof"
)

// send down a single random document
func randDocument(w http.ResponseWriter, r *http.Request) {

	// generate a random number
	docId := rand.Intn(docs.number)
	item := docs.docMap[docs.docList[docId]]

	// serialize the item before sending it out
	bytes, err := json.Marshal(item)
	if err != nil {
		fmt.Fprintf(w, "Failed to marshal document %v", err)
		return
	}
	fmt.Fprintf(w, string(bytes)) // send data to client side
}

// send a million documents
func million(w http.ResponseWriter, r *http.Request) {

	for i := 0; i < 10000000; i++ {
		docId := i % docs.number
		item := docs.docMap[docs.docList[docId]]
		bytes, err := json.Marshal(item)
		if err != nil {
			fmt.Fprintf(w, "Failed to marshal document %v", err)
			return
		}
		fmt.Fprintf(w, string(bytes)) // send data to client side
		fmt.Fprintf(w, "\n\n")
	}
}

// send a million string documents
func millionstr(w http.ResponseWriter, r *http.Request) {

	for i := 0; i < 10000000; i++ {
		docId := i % docs.number
		bytes := docs.docMapStr[docs.docList[docId]]
		fmt.Fprintf(w, bytes) // send data to client side
		fmt.Fprintf(w, "\n\n")
	}
}

var server = flag.String("server", "http://localhost:9000",
	"couchbase server URL")
var bucket = flag.String("bucket", "beer-sample", "bucket name")

type documents struct {
	docMap    map[string]interface{}
	docMapStr map[string]string
	docList   []string
	number    int
}

var docs *documents

func main() {

	flag.Parse()

	http.HandleFunc("/random", randDocument) // set router
	http.HandleFunc("/million", million)
	http.HandleFunc("/millionstr", millionstr)

	docMap := fetch.FetchDocs(*server, *bucket)
	if len(docMap) == 0 {
		log.Fatalf("Failed to fetch documents")
	}

	docs = &documents{docMap: docMap,
		docList:   make([]string, 0, len(docMap)),
		number:    len(docMap),
		docMapStr: make(map[string]string)}

	for dName, value := range docs.docMap {
		docs.docList = append(docs.docList, dName)
		item, _ := json.Marshal(value)
		docs.docMapStr[dName] = string(item)
	}

	err := http.ListenAndServe(":9090", nil) // set listen port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
