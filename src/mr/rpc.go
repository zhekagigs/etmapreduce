package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MessageType int32

const (
	Idle       MessageType = 0
	InProgress MessageType = 1
	Completed  MessageType = 2
	Reduce     MessageType = 3
	Map        MessageType = 4
)

func messageTypeString(t MessageType) string {
	switch t {
	case Idle:
	  return "Idle"
	case InProgress:
	  return "In Progress"
	case Completed:
	  return "Completed"
	case Reduce:
	  return "Reduce"
	case Map:
	  return "Map"
	default:
	  return fmt.Sprintf("Unknown MessageType: %d", t)
	}
  }
  

type ExampleArgs struct {
	X     int
	Hello string
}

type ExampleReply struct {
	Y     int
	World string
}

type TaskRequest struct {
	State      MessageType
	FileNames  []string
	TaskNumber int
	TaskType MessageType
}

type TaskReply struct {
	Filename    string
	TaskNumber  int
	NumReducers int
	TaskType MessageType

}

func (t TaskReply) String() string {
    return fmt.Sprintf("Filename: %s, Task Number: %d, Num Reducers: %d, Task Type: %s",
        t.Filename, t.TaskNumber, t.NumReducers, messageTypeString(t.TaskType))
}

 
type Task struct {
	Filename  string
	TaskNum   int
	isMapped  bool
	isReduced bool
	State     MessageType
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
