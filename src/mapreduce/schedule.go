package mapreduce

import (
	"fmt"
	"sync"
	"log"
)

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


	var wg sync.WaitGroup
	for id := 0; id < ntasks; id++ {

		log.Println("----------", id)
		wg.Add(1) // 每次开启新的goroutine时，增加1
		go func(id int) {
			defer wg.Done() // goroutine结束时，减一

			// 对于同一task 会反复去分配worker 处理 worker fail
			//for {
				addr := <- registerChan // 取出worker addr 要重新放回
				log.Println("worker addr: ", addr, "id: ", id, "phase", phase)
				fileName := ""
				if phase == mapPhase {
					fileName = mapFiles[id]
				}

				tmp := DoTaskArgs{JobName:jobName, File:fileName, Phase:phase, TaskNumber: id, NumOtherPhase:n_other}
				call(addr, "Worker.DoTask", tmp, nil)

				//if !ok {
				//	// worker没有响应，继续分配
				//	log.Println("DoTask error", addr)
				//} else {
				//	// worker响应，break，处理下一个task


					// 需要goroutine，否则死锁，part III 就需要
					go func() { // 重新将addr加入Chan
						registerChan <- addr
					}()


					//break
//				}
			//}

		}(id)
	}

	wg.Wait() // 等待所有goroutine 结束
	fmt.Printf("Schedule: %v phase done\n", phase)
}
