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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type JobType int

const (
	Map JobType = iota
	Reduce
	Wait
	Done
)

type NotifyReply struct {
	Job                    JobType
	JobId                  int
	InputFileForMapJob     string
	NumReduceJobs          int
	InputFilesForReduceJob []string
}

type NotifyArgs struct {
	WorkerId            int
	Job                 JobType
	MapJobOutputFiles   []string
	ReduceJobOutputFile string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
