package main

import (
	"fmt"
	"math/rand"
	"time"
)

/*
Worker work work and yells work completed whenever it finishes.
*/
type Worker struct {
	request chan Work
}

func (worker *Worker) work(ready chan *Worker) {
	ready <- worker
	for {
		work := <-worker.request
		fmt.Println("Worker work work")
		work.c <- work.work()
		fmt.Println("Work completed")
		ready <- worker
	}
}

/*
Master stores works in a queue and dispatch work to any available workers
*/
type Master struct {
	queue   []Work
	workers []*Worker
	ready   chan *Worker
}

func (master *Master) balance(request chan Work) {
	for {
		select {
		case work := <-request:
			master.store(work)
		case worker := <-master.ready:
			master.dispatch(worker)
		}
	}
}

func (master *Master) store(work Work) {
	master.queue = append(master.queue, work)
}

func (master *Master) dispatch(worker *Worker) {
	work := master.queue[0]
	master.queue = master.queue[1:]

	fmt.Printf("Dispatching work %s\n", work.name)

	worker.request <- work
}

/*
Work struct to carry work to be done

Right now it is just carry a dummy function that returns a integer, I want to
abstract out the work being carried a little more useful later
*/
type Work struct {
	name string
	c    chan string
	work func() string
}

func main() {
	var queue []Work
	var workers []*Worker
	ready := make(chan *Worker)
	request := make(chan Work)
	done := make(chan string)

	for i := 0; i < 5; i++ {
		workers = append(workers, &Worker{
			request,
		})
	}

	master := Master{
		queue,
		workers,
		ready,
	}

	go func() {
		for i := 0; i < 1000; i++ {
			master.store(Work{
				fmt.Sprintf("Test %d", i),
				done,
				func() string {
					time.Sleep(time.Duration(rand.Intn(5)) * time.Second)
					return fmt.Sprintf("Random task")
				},
			})
		}
	}()

	go master.balance(request)

	for i := 0; i < 5; i++ {
		go workers[i].work(ready)
	}

	for {
		fmt.Println(<-done)
	}
}
