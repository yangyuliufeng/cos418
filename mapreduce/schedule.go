package mapreduce

import "time"

// As tasks become available, schedule() decides how to assign those tasks to workers, and how to handle worker failures.
// schedule() starts and waits for all tasks in the given phase (Map or Reduce).
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

	stats := make([]bool, ntasks)
	current_worker := 0

	for {
		count := ntasks
		for i := 0; i < ntasks; i++ {
			if !stats[i] {
				mr.Lock()
				num_workers := len(mr.workers)
				if num_workers == 0 {
					mr.Unlock()
					continue
				}
				current_worker = (current_worker + 1) % num_workers
				Worker := mr.workers[current_worker]
				mr.Unlock()
				var file string
				if phase == mapPhase {
					file = mr.files[i]
				}
				args := DoTaskArgs{JobName: mr.jobName, File: file, Phase: phase, TaskNumber: i, NumOtherPhase: nios}
				go func(slot int, worker_ string) {
					success := call(worker_, "Worker.DoTask", &args, new(struct{}))
					if success {
						stats[slot] = true
					}
				}(i, Worker)
			} else {
				count--
			}
		}
		if count == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	debug("Schedule: %v phase done\n", phase)
}

