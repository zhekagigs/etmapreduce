package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

/*
The workers will talk to the coordinator via RPC.
Each worker process will ask the coordinator for a task,
read the task's input from one or more files, execute the task,
and write the task's output to one or more files.
The coordinator should notice
if a worker hasn't completed its task in a reasonable amount of time
(for this lab, use ten seconds),
and give the same task to a different worker.
*/

type Coordinator struct {
	// Your definitions here.
	Tasks          []Task
	NumReduce      int
	MapTaskCode    int
	ReduceTaskCode int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GiveMeAMapTask(request *TaskRequest, reply *TaskReply) error {
	fmt.Println("Coordinator::Map requested")
	for _, task := range c.Tasks {
		if !task.isMapped {
			reply.Filename = task.Filename
		}
	}
	return nil
}

// func (c *Coordinator) giveMeAReduceTask(request *TaskRequest, reply *TaskReply) error {
// 	fmt.Println("Coordinator::Reduce requested")
// 	for _, task := range c.Tasks {
// 		if !task.isReduced {
// 			reply.Filename = task.Filename
// 		}
// 	}
// 	return nil
// }

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Println("Coordinator::Example hande")
	reply.Y = args.X + 11
	reply.World = " It's world, my man "
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("Coordinator::Server listens on sockname")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	result := true
	for _, task := range c.Tasks {
		if !task.isMapped || !task.isReduced {
			result = false
		}
	}
	// Your code here.
	// log.Println("Coordinator::Done called")

	return result
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var tasks []Task
	for _, filename := range files {
		tasks = append(tasks, Task{filename, false, false})
	}
	c := Coordinator{
		Tasks:          tasks,
		NumReduce:      nReduce,
		MapTaskCode:    0,
		ReduceTaskCode: 0,
	}

	log.Println("Coordinator:: files to work", tasks)
	log.Println("Coordinator:: new coordinator made v.01")

	c.server()
	return &c
}
