package mapreduce

import (
	"fmt"
	//"sync"
	"log"
	"sync"
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
func schedule1(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
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

	flag := make(chan bool, ntasks) // buffer 容量为n

	for id := 0; id < ntasks; id++ {

		log.Println("----------", id)
		go func(id int) {
			//go func(id int, flag chan bool, registerChan chan string) {
			// 6，chan 作为goroutine之间的信道，在匿名goroutine中，不用作为形参传入，但是其他变量需要，如id。

			fileName := ""
			if phase == mapPhase {
				fileName = mapFiles[id]
			}

			// 对于同一task 会反复去分配worker 处理 worker fail Part IV
			for {

				addr := <- registerChan // 取出worker addr 要重新放回

				// 若 registerChan 没有work，则阻塞等待

				task := DoTaskArgs{JobName:jobName, File:fileName, Phase:phase, TaskNumber: id, NumOtherPhase:n_other}
				ok := call(addr, "Worker.DoTask", task, nil)

				if !ok {
					// worker没有响应，work addr不再放入 Channel，由master处理worker的failure
					// 将task 分配给其他 worker

					log.Println("DoTask error", addr)
				} else {
					// worker响应，break，处理下一个task

					flag <- true
					registerChan <- addr

					break
				}
			}

		}(id)
	}

	for i := 0; i < ntasks; i++ {
		<- flag
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

// 1，flag 与 sync.WaitGroup 等价
// 2, registerChan 没有缓存buffer，在其他地方有存入操作

// 6，chan 作为goroutine之间的信道，在匿名goroutine中，不用作为形参传入，但是其他变量需要。
// 7，race conditions 竞态条件，多个线程企图同时修改一个变量，若没有并发控制，则x的取值受线程顺序的影响。


/* 3, debug：没有flag，会出现 worker分配多个任务
    已解决：因为schedule会被调用两次，先是Schedule(map)中所有map task执行完，再开始Schedule(reduce).
    即 Schedule要等待所有的task结束，再return。这个思路是，func A中有匿名goroutine B，显然，A得等B结束再return。

*/

/* 4, debug：有flag，顺序影响了结果
    已解决：初始时，registerChan是空的，flag也是空的

    Schedule要结束，需等待获得flag中nReduce个内容；
    如果registerChan <- addr 在 flag <- true 前面，考虑最后两个task A B的事件线：
    mr.forwardRegistrations 填入work1；A 获得 work1；
    mr.forwardRegistrations 填入work2；B 获得 work2；
    A结束，释放registerChan，填入work1；
    B结束，由于registerChan已满，无法填入，阻塞。故flag也无法填入，即goroutine与Schedule都无法结束。
    死锁。
*/

// 5, debug: 用sync.WaitGroup，则registerChan的 存入 必须使用 goroutine

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




			fileName := ""
			if phase == mapPhase {
				fileName = mapFiles[id]
			}

			// 对于同一task 会反复去分配worker 处理 worker fail Part IV
			for {

				addr := <- registerChan // 取出worker addr 要重新放回

				task := DoTaskArgs{JobName:jobName, File:fileName, Phase:phase, TaskNumber: id, NumOtherPhase:n_other}
				ok := call(addr, "Worker.DoTask", task, nil)

				if !ok {
					log.Println("DoTask error", addr)
				} else {
					go func() { // 必须 使用 goroutine 因为最后一个task可能无法存入registerChan
						registerChan <- addr
					}()
					break
				}
			}

		}(id)
	}

	wg.Wait() // 等待所有goroutine 结束
	fmt.Printf("Schedule: %v phase done\n", phase)
}
