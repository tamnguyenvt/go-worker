package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Provide a list of items need to be processed here
// TODO: Please edit this function
func getList() []int {
	arr := make([]int, 10)
	for i := range arr {
		arr[i] = i + 1
	}

	return arr
}

// This is a sample function. Basically its currently try to simulate time to finish a work (randomly from 1-10s)
// TODO: Please edit this function
func worker(ctx context.Context, item interface{}) {
	min := 1
	max := 10
	t := rand.Intn(max - min) + min
	time.Sleep(time.Duration(t) * time.Second)
}

func main()  {
	relaxAfter := 1 * time.Minute
	relaxDuration := 3 * time.Second
	ch, wm := NewWorkerManager(WorkerManagerParams{
		WorkerSize: 5,
		RelaxAfter: &relaxAfter,
		RelaxDuration: &relaxDuration,
		WorkerFunc: worker,
		LogEnable: true,
		StopTimeout: -1,
	})

	wm.Start()
	items := getList()
	index := 0
	for index < len(items) {
		select {
		case ch <- items[index]:
			index++
		default:
			// Do nothing
		}
	}

	wm.Stop()
}

type WorkerManagerParams struct {
	WorkerSize int
	RelaxAfter *time.Duration
	RelaxDuration *time.Duration
	WorkerFunc func(ctx context.Context, item interface{})
	LogEnable  bool
	StopTimeout int // In seconds, Put -1 to wait forever.
}
type WorkerManager struct{
	sync.Mutex
	current int
	finished 	int
	failed 		int
	start time.Time
	itemChan chan interface{}
	conf WorkerManagerParams
}

func NewWorkerManager(params WorkerManagerParams) (itemChan chan interface{}, wm *WorkerManager) {
	itemChanTemp := make(chan interface{})

	return itemChanTemp, &WorkerManager{
		conf:     params,
		itemChan: itemChanTemp,
	}
}

func (t *WorkerManager) Start()  {
	t.start = time.Now()
	go func() {
		for item := range t.itemChan {
			for !t.join(item) {
				// You may want to wait for a second before retry
			}
		}
	}()
}

func (t *WorkerManager) Stop()  {
	close(t.itemChan)
	stopTime := time.Now()
	for t.current > 0 {
		if t.conf.StopTimeout > 0 && time.Now().Add(time.Duration(-1 * t.conf.StopTimeout) * time.Second).After(stopTime) {
			t.log("Timeout!. Stopped worker!")
			return
		}
	}
	t.log("Stopped worker gracefully!")

}

func (t *WorkerManager) join(item interface{}) bool {
	t.Lock()
	defer t.Unlock()

	if t.conf.RelaxAfter != nil && time.Since(t.start) > *t.conf.RelaxAfter{
		relaxDuration := 3 * time.Second
		if t.conf.RelaxDuration != nil {
			relaxDuration = *t.conf.RelaxDuration
		}
		t.log("Relaxing after ", t.conf.RelaxAfter.Minutes(), " minutes. ===============")
		time.Sleep(relaxDuration)
		t.start = time.Now()
	}

	if t.current == t.conf.WorkerSize {
		return false
	}

	t.current = t.current + 1
	t.log("Pickup ", item, ", running ", t.current, " workers")

	go func(item interface{}) {
		defer func() {
			if err := recover(); err != nil {
				t.log("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - failed to process item: ", item)
				t.fail()
			}
		}()
		t.conf.WorkerFunc(context.Background(), item)
		t.finish()

	}(item)

	return true
}

func (t *WorkerManager) finish() {
	t.Lock()
	defer t.Unlock()

	t.current = t.current - 1
	t.finished = t.finished + 1

	t.log(fmt.Sprintf("Finished/Failed count: %d/%d, running %d", t.finished, t.failed, t.current))
}

func (t *WorkerManager) fail() {
	t.Lock()
	defer t.Unlock()

	t.current = t.current - 1
	t.failed = t.failed + 1

	t.log(fmt.Sprintf("Finished/Failed count: %d/%d, running %d", t.finished, t.failed, t.current))
}

func (t *WorkerManager) log(params ...interface{})  {
	if t.conf.LogEnable {
		log.Print(params...)
	}
}

func (t *WorkerManager) getCurrent() int {
	t.Lock()
	defer t.Unlock()

	return t.current
}