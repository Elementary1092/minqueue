package main

import (
	"container/list"
	"context"
	"flag"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var port uint64

func init() {
	flag.Uint64Var(&port, "p", 8000, "set a port on which a server runs (should be less than 65535)")
}

func main() {
	flag.Parse()

	if port > math.MaxUint16 {
		panic("invalid port number")
	}

	http.HandleFunc("GET /{queue}", FromQueue)
	http.HandleFunc("PUT /{queue}", ToQueue)
	_ = http.ListenAndServe(":"+strconv.FormatUint(port, 10), nil)
}

var manager = &QueueManager{queues: sync.Map{}}

func FromQueue(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queue")

	timeout := 0
	urlQuery := r.URL.Query() // query is parsed on every call so, saving query parsing result to avoid such behaviour
	if urlQuery.Has("timeout") {
		timeoutRaw := urlQuery.Get("timeout")
		if timeoutRaw == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		t, err := strconv.ParseInt(timeoutRaw, 10, 64)
		if err != nil || t < 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		timeout = t
	}

	ctx, cancel = context.WithTimeout(r.Context(), time.Duration(timeout)*time.Second)
	defer cancel()

	data := manager.PopFrom(ctx, queueName)
	if data == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_, err := w.Write([]byte(data))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func ToQueue(w http.ResponseWriter, r *http.Request) {
	queueName := r.PathValue("queue")

	urlQuery := r.URL.Query() // query is parsed on every call so, saving query parsing result to avoid such behaviour
	if !urlQuery.Has("v") {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	data := urlQuery.Get("v")
	if data == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	manager.PushTo(queueName, data)

	w.WriteHeader(http.StatusOK)
}

type Queue struct {
	data *list.List
	mu   sync.Mutex
}

func (q *Queue) PopFront() string {
	q.mu.Lock()
	defer q.mu.Unlock()

	data := q.data.Front()
	if data == nil {
		return ""
	}

	q.data.Remove(data)

	return data.Value.(string) // should never panic because only strings are added to the queue
}

func (q *Queue) PushBack(value string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.data.PushBack(value)
}

type QueueWithWait struct {
	*Queue
	wait sync.Mutex
}

func (q *QueueWithWait) PopFront(ctx context.Context) string {
	select {
	case <-ctx.Done():
		return q.Queue.PopFront()
	default:
	}

	q.wait.Lock()
	defer q.wait.Unlock()

	for q.data.Len() == 0 {
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(time.Second): // used to check the length of the queue regularly and reduce the load on CPU
			continue
		}
	}

	return q.Queue.PopFront()
}

func (q *QueueWithWait) PushBack(value string) {
	q.Queue.PushBack(value)
}

type QueueManager struct {
	queues sync.Map
}

func (q *QueueManager) PopFrom(ctx context.Context, queueName string) string {
	mappedQueue, ok := q.queues.Load(queueName)
	if !ok {
		return ""
	}
	queue, _ := mappedQueue.(*QueueWithWait) // to satisfy the linter

	return queue.PopFront(ctx)
}

func (q *QueueManager) PushTo(queueName string, data string) {
	mapQueue, ok := q.queues.Load(queueName)
	if !ok {
		queue := &QueueWithWait{Queue: &Queue{data: list.New(), mu: sync.Mutex{}}, wait: sync.Mutex{}}
		q.queues.Store(queueName, queue)
		mapQueue = queue
	}
	queue, _ := mapQueue.(*QueueWithWait) // to satisfy the linter

	queue.PushBack(data)
}
