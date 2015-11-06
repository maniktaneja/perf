package main

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"runtime"

	"github.com/oxtoacart/bpool"
)

var (
	MaxWorker = runtime.NumCPU() * 2
	MaxQueue  = os.Getenv("MAX_QUEUE")
)

var bufpool = bpool.NewBufferPool(1024)

// Job represents the job to be run
type Job struct {
	raw   interface{}
	outch chan Result
}

type Result struct {
	data    *bytes.Buffer
	bufpool *bpool.BufferPool
}

func (j *Job) MarshalData() {

	result := Result{
		bufpool: bufpool,
		data:    bufpool.Get(),
	}

	result.data = bufpool.Get()
	err := json.NewEncoder(result.data).Encode(&j.raw)
	if err != nil {
		log.Fatalf("Error %v", err)
	}

	j.outch <- result
}

// A buffered channel that we can send work requests on.
var JobQueue = make(chan Job, 1024)

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	quit       chan bool
}

func NewWorker(workerPool chan chan Job) Worker {
	return Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		quit:       make(chan bool)}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		log.Printf(" Starting worker ... ")
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				job.MarshalData()

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	maxWorkers int
}

func NewDispatcher() *Dispatcher {
	pool := make(chan chan Job, MaxWorker)
	return &Dispatcher{WorkerPool: pool, maxWorkers: MaxWorker}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}

	go d.dispatch()
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}
