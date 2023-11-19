package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MessageType int32

const (
	Idle       MessageType = 0
	InProgress MessageType = 1
	Completed  MessageType = 2
)

type ExampleArgs struct {
	X     int
	Hello string
}

type ExampleReply struct {
	Y     int
	World string
}

type TaskRequest struct {
	State MessageType
}

type TaskReply struct {
	Filename    string
	TaskNumber  int
	NumReducers int
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
