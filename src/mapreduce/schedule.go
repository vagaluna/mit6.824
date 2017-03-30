package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	var finished = 0
	var scheduled = 0
	var finishChan = make(chan string)

	for {
		var readyWorker string
		select {
		case readyWorker = <-registerChan:

		case readyWorker = <-finishChan:
			finished = finished + 1
		}
		if finished == ntasks {
			break
		}
		if scheduled < ntasks {
			var inFile string
			if phase == mapPhase {
				inFile = mapFiles[scheduled]
			} else {
				inFile = ""
			}
			go sendTaskToWorker(jobName, readyWorker, inFile, phase, scheduled, n_other, finishChan)
			scheduled = scheduled + 1
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func sendTaskToWorker(jobName string, wk string, inFile string,
	phase jobPhase, taskIndex int, nOther int,
	finishChan chan string) {

	var args = DoTaskArgs{jobName, inFile, phase, taskIndex, nOther}
	var _ = call(wk, "Worker.DoTask", args, nil)
	finishChan <- wk
}
