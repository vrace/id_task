package idtask

import (
	"runtime"
	"sync"
)

type IdTaskTransform[T any] func(id int) T

type IdTaskOutput[T any] interface {
	OutputBegin()
	OutputUpdate(value T)
	OutputFinish()
}

type IdTask[T any] struct {
	transform  IdTaskTransform[T]
	output     IdTaskOutput[T]
	numWorkers int
	inputChan  chan int
	outputChan chan T
}

func New[T any](transform IdTaskTransform[T], output IdTaskOutput[T], numWorkers int) IdTask[T] {
	if numWorkers <= 0 {
		numWorkers = min(16, runtime.NumCPU()*2)
	}
	return IdTask[T]{
		transform:  transform,
		output:     output,
		numWorkers: numWorkers,
	}
}

func (task *IdTask[T]) Execute(numItems int) {
	task.ExecuteMT(numItems)
}

func (task *IdTask[T]) ExecuteST(numItems int) {
	task.output.OutputBegin()
	for i := 0; i < numItems; i++ {
		task.output.OutputUpdate(task.transform(i))
	}
	task.output.OutputFinish()
}

func (task *IdTask[T]) ExecuteMT(numItems int) {
	if task.inputChan != nil || task.outputChan != nil {
		panic("the task was not properly closed yet")
	}

	task.inputChan = make(chan int, task.numWorkers)
	task.outputChan = make(chan T, task.numWorkers)

	var waitTransforms sync.WaitGroup
	for i := 0; i < task.numWorkers; i++ {
		waitTransforms.Add(1)
		go func() {
			defer waitTransforms.Done()
			task.ExecuteTransformWorker()
		}()
	}

	waitOutputs := make(chan int)
	go func() {
		defer func() {
			waitOutputs <- 0
		}()
		task.ExecuteOutput()
	}()

	for i := 0; i < numItems; i++ {
		task.inputChan <- i
	}
	close(task.inputChan)

	waitTransforms.Wait()
	close(task.outputChan)

	<-waitOutputs

	task.inputChan = nil
	task.outputChan = nil
}

func (task *IdTask[T]) ExecuteTransformWorker() {
	for id := range task.inputChan {
		task.outputChan <- task.transform(id)
	}
}

func (task *IdTask[T]) ExecuteOutput() {
	task.output.OutputBegin()
	for value := range task.outputChan {
		task.output.OutputUpdate(value)
	}
	task.output.OutputFinish()
}
