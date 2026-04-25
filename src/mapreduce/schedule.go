package mapreduce

import (
	// "fmt"
	"net/rpc"
	"sync"
	"sync/atomic"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// TODO (Parts D and E): dispatch all ntasks tasks to workers and return
	// only once every task has completed successfully.
	//
	// Workers are available via mr.registerChannel. Read a worker address
	// before dispatching a task, and return it to the channel when the task
	// finishes (or fails) so other tasks can reuse it. Note that
	// mr.registerChannel is unbuffered, so return the address asynchronously
	// to avoid a deadlock on the last task in the phase.
	
	var workerGroup sync.WaitGroup
	done := make([]int32, ntasks) // 0 is not done, 1 is done. and slice initialization always has 0

	runTask := func (taskNumber int) {
		file := ""
		if phase == mapPhase {
			file = mr.files[taskNumber]
		}
		taskArgs := DoTaskArgs {
			JobName: mr.jobName,    
			File: file,
			Phase: phase,   
			TaskNumber: taskNumber,
			NumOtherPhase: nios,
		}

		for {
				workerAddress := <- mr.registerChannel
				workerConnection, err := rpc.Dial("unix", workerAddress)
				if  err == nil {
				} else {
					go func() {
						// registerChannel is unbuffered
						mr.registerChannel <- workerAddress
					}()
					continue
				}

				var reply struct{}
				err = workerConnection.Call("Worker.DoTask",taskArgs, &reply)
				
				if  err == nil {
					if atomic.CompareAndSwapInt32(&done[taskNumber], 0, 1) {
						workerGroup.Done()
					}
					
					go func() {
						// registerChannel is unbuffered
						mr.registerChannel <- workerAddress
					}()
					break
				} else {
					go func() {
						// registerChannel is unbuffered
						mr.registerChannel <- workerAddress
					}()
					continue
				}
			}
	}

	straggleHandler := func (taskAssigned int) {
		for taskNumber := range taskAssigned {
			if atomic.LoadInt32(&done[taskNumber]) == 0 {
					go func(taskNumber int) {
						runTask(taskNumber)
					}(taskNumber)
				}
		}
	}

	for taskNumber := range ntasks {
		
		workerGroup.Add(1)
		go func(taskNumber int) {
			runTask(taskNumber)
		}(taskNumber)

		go func(taskNumber int) {
			straggleHandler(taskNumber)
		}(taskNumber)

	}

	
	// TODO (Part F): when all but one task have completed, launch backup tasks
	// for any that are still running, to avoid being held up by a slow worker.
	//
	// Consider: how do you track which individual tasks are still in progress?
	// A shared count won't be enough. Also consider what happens if both the
	// original and a backup copy of a task finish successfully.
	workerGroup.Wait()
	debug("Schedule: %v phase done\n", phase)
}
