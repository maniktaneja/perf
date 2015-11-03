// fetch documents from a bucket using

// couchbase datastore API

package fetch

import (
	"github.com/couchbase/query/datastore"
	"github.com/couchbase/query/datastore/couchbase"
	"github.com/couchbase/query/logging"
	log_resolver "github.com/couchbase/query/logging/resolver"
	"log"
	"math"
)

func FetchDocs(serverURL string, bucketName string) map[string]interface{} {

	logger, _ := log_resolver.NewLogger("golog")
	if logger == nil {
		log.Fatalf("Invalid logger")
	}

	logging.SetLogger(logger)
	site, err := couchbase.NewDatastore(serverURL)
	if err != nil {
		log.Fatalf("Cannot create datastore %v", err)
	}

	namespace, err := site.NamespaceByName("default")
	if err != nil {
		log.Fatalf("Namespace default not found, error %v", err)
	}

	ks, err := namespace.KeyspaceByName(bucketName)
	if err != nil {
		log.Fatalf(" Cannot connect to %s. Error %v", bucketName, err)
	}

	indexer, err := ks.Indexer(datastore.VIEW)
	if err != nil {
		log.Fatalf("No view indexer found %v", err)
	}

	// try create a primary index
	index, err := indexer.CreatePrimaryIndex("", "#primary", nil)
	if err != nil {
		// keep going. maybe index already exists
		log.Printf(" Cannot create a primary index on bucket. Error %v", err)
		pi, err := indexer.PrimaryIndexes()
		if err != nil || len(pi) < 1 {
			log.Fatalf("No primary index found")
		}
		index = pi[0]
	} else {
		log.Printf("primary index created %v", index)
	}

	conn := datastore.NewIndexConnection(nil)
	go index.ScanEntries("", math.MaxInt64, datastore.UNBOUNDED, nil, conn)

	var entry *datastore.IndexEntry
	var fetchKeys = make([]string, 0, 1000)

	ok := true
	for ok {

		select {
		case entry, ok = <-conn.EntryChannel():
			if ok {
				fetchKeys = append(fetchKeys, entry.PrimaryKey)
			}
		}
	}

	//fetch all the keys
	pairs, errs := ks.Fetch(fetchKeys)
	if errs != nil {
		log.Fatalf(" Failed to fetch keys %v", errs)
	}

	var keyMap = make(map[string]interface{})
	for _, value := range pairs {
		keyMap[value.Key] = value.Value.Actual()
	}

	log.Printf("Got %v docs", len(keyMap))

	return keyMap
}
