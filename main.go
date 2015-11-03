package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gocb"
	"github.com/maniktaneja/perf/fetch"
)

var serverURL = flag.String("serverURL", "http://localhost:9000",
	"couchbase server URL")
var poolName = flag.String("poolName", "default",
	"pool name")
var bucketName = flag.String("bucketName", "default",
	"bucket name")
var set = flag.Bool("set", false, "create document mode")
var size = flag.Int("size", 1024, "document size")
var documents = flag.Int("documents", 2000000, "total documents")
var threads = flag.Int("threads", 10, "Number of threads")
var quantum = flag.Int("quantum", 1024, "Number of documents per bulkGet")
var engine = flag.String("engine", "go-couchbase", "one of gocb or go-couchbase")

var wg sync.WaitGroup

func maybeFatal(err error) {
	if err != nil {
		log.Fatalf("Error:  %v", err)
	}
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*threads)

	var client cbclient

	if *engine == "gocb" {
		client = newGoCbClient(*serverURL, *bucketName)
	} else {
		client = newGoCouchbaseClient(*serverURL, *bucketName)
	}

	start := time.Now()
	if *set == true {
		jsonDocs := fetch.FetchDocs(*serverURL, "beer-sample")
		if len(jsonDocs) == 0 {
			log.Fatalf("Fetched 0 docs ")
		}
		for i := 0; i < *threads; i++ {
			go client.doSetOps(*documents / *threads, i*(*documents / *threads), jsonDocs)
			wg.Add(1)
		}
	} else {
		for i := 0; i < *threads; i++ {
			go client.doBulkGetOps(*documents / *threads, *quantum, i*(*documents / *threads))
			wg.Add(1)
		}
	}

	wg.Wait()

	finish := time.Now().Sub(start)
	fmt.Printf("**** Did %d ops in %s. Ops/sec %d\n",
		*documents, finish.String(), int(float64(*documents)/finish.Seconds()))

}

type cbclient interface {
	doBulkGetOps(int, int, int)                // total, quantum, startNum
	doSetOps(int, int, map[string]interface{}) // total, startNum, key-map
}

type goCouchbaseClient struct {
	bucket *couchbase.Bucket
}

func newGoCouchbaseClient(serverURL string, bucket string) *goCouchbaseClient {
	client, err := couchbase.Connect(serverURL)
	if err != nil {
		log.Fatalf("Connect failed %v", err)
	}

	cbpool, err := client.GetPool("default")
	if err != nil {
		log.Fatalf("Failed to connect to default pool %v", err)
	}

	cbbucket, err := cbpool.GetBucket(bucket)
	if err != nil {
		log.Fatalf("Unable to connect to bucket %s. Error %v", bucketName, err)
	}

	return &goCouchbaseClient{bucket: cbbucket}

}

func (gcb *goCouchbaseClient) doBulkGetOps(total int, quantum int, startNum int) {

	defer wg.Done()
	start := time.Now()
	iter := total / quantum
	currentKeyNum := startNum
	for i := 0; i < iter; i++ {

		keylist := make([]string, quantum, quantum)
		for j := 0; j < quantum; j++ {
			key := fmt.Sprintf("test%d", currentKeyNum)
			keylist[j] = key
			currentKeyNum++

		}
		_, err := gcb.bucket.GetBulk(keylist)
		if err != nil {
			log.Printf(" Failed to get keys startnum %s to %d", keylist[0], quantum)
		}
	}
	fmt.Printf("Did %d ops in %s\n",
		total, time.Now().Sub(start).String())
}

func (gcb *goCouchbaseClient) doSetOps(total int, startNum int, kv map[string]interface{}) {

	defer wg.Done()

	keys := make([]string, 0, len(kv))
	for key, _ := range kv {
		keys = append(keys, key)
	}

	start := time.Now()
	for i := 0; i < total; i++ {

		key := keys[i%len(kv)]
		data := kv[key]
		k := fmt.Sprintf("test%d", startNum+i)
		maybeFatal(gcb.bucket.Set(k, 0, data))
	}
	fmt.Printf("Did %d ops in %s\n",
		total, time.Now().Sub(start).String())
}

type goCbClient struct {
	bucket *gocb.Bucket
}

func newGoCbClient(serverURL string, bucketName string) *goCbClient {
	cluster, err := gocb.Connect(serverURL)
	if err != nil {
		log.Fatalf("Could not connect to %v.  Err: %v", serverURL, err)
	}
	bucket, err := cluster.OpenBucket(bucketName, "")
	if err != nil {
		log.Fatalf("Could not open bucket: %v.  Err: %v", bucketName, err)
	}

	return &goCbClient{
		bucket: bucket,
	}
}

func (gcb *goCbClient) doSetOps(total int, startNum int, kv map[string]interface{}) {

	defer wg.Done()

	keys := make([]string, 0, len(kv))
	for key, _ := range kv {
		keys = append(keys, key)
	}

	start := time.Now()
	for i := 0; i < total; i++ {

		key := keys[i%len(kv)]
		data := kv[key]
		k := fmt.Sprintf("test%d", startNum+i)
		if _, err := gcb.bucket.Insert(k, data, 0); err != nil {
			log.Fatalf("Failed to insert %v", err)
		}
	}
	fmt.Printf("Did %d ops in %s\n",
		total, time.Now().Sub(start).String())
}

func (gcb *goCbClient) doBulkGetOps(total int, quantum int, startNum int) {

	defer wg.Done()
	start := time.Now()
	iter := total / quantum
	currentKeyNum := startNum

	// Create a JSON document
	type Doc struct {
		Item string `json:"item"`
	}

	for i := 0; i < iter; i++ {

		var itemsGet []gocb.BulkOp
		for j := 0; j < quantum; j++ {
			key := fmt.Sprintf("test%d", currentKeyNum)
			itemsGet = append(itemsGet, &gocb.GetOp{Key: key, Value: &Doc{}})
			currentKeyNum++

		}
		err := gcb.bucket.Do(itemsGet)
		if err != nil {
			log.Fatalf("ERRROR PERFORMING BULK GET:", err)
		}
	}
	fmt.Printf("Did %d ops in %s\n",
		total, time.Now().Sub(start).String())
}
