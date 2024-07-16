package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type PendingJob struct {
	jobId     int
	startTime int
}

type Coordinator struct {
	// Your definitions here.
	numReduceJobs int
	inputFiles    []string

	mu             sync.Mutex
	pendingWorkers map[int]PendingJob // worker id -> job id, timestamp
	pendingJobs    map[int]bool
	finishedJobs   map[int]bool

	numFinishedMapJobs        int
	outputFilesFromMapJobs    [][]string
	outputFilesFromReduceJobs map[string]bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) logStatus() {
	jCoordinator, _ := json.MarshalIndent(c, "", "\t")
	log.Println(string(jCoordinator))
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Notify(args *NotifyArgs, reply *NotifyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.notifyFinish(args)
	c.assignJob(args, reply)

	return nil
}

func (c *Coordinator) notifyFinish(resource *NotifyArgs) {
	if resource.Job == Map || resource.Job == Reduce {
		Job, ok := c.pendingWorkers[resource.WorkerId]
		// could be timed-out worker, so check it's legit
		if ok {
			c.finishedJobs[Job.jobId] = true
			delete(c.pendingWorkers, resource.WorkerId)
			delete(c.pendingJobs, Job.jobId)
			c.saveWorkerOutput(resource, Job.jobId)
		}
	}
}

func (c *Coordinator) saveWorkerOutput(resource *NotifyArgs, jobid int) {
	switch resource.Job {
	case Map:
		for reduceJobId, file := range resource.MapJobOutputFiles {
			c.outputFilesFromMapJobs[reduceJobId] = append(c.outputFilesFromMapJobs[reduceJobId], file)
		}
		c.numFinishedMapJobs += 1
		if c.numFinishedMapJobs == len(c.inputFiles) {
			c.finishedJobs = make(map[int]bool)
		}
	case Reduce:
		c.outputFilesFromReduceJobs[resource.ReduceJobOutputFile] = true
		for _, file := range c.outputFilesFromMapJobs[jobid] {
			e := os.Remove(file)
			if e != nil {
				log.Fatal(e)
			}
		}
	}
}

func (c *Coordinator) assignJob(resource *NotifyArgs, reply *NotifyReply) {
	if len(c.outputFilesFromReduceJobs) == c.numReduceJobs {
		reply.Job = Done
		return
	}

	reply.NumReduceJobs = c.numReduceJobs
	numJobs := 0
	if c.numFinishedMapJobs < len(c.inputFiles) {
		numJobs = len(c.inputFiles)
		reply.Job = Map
	} else {
		numJobs = c.numReduceJobs
		reply.Job = Reduce
	}

	// assign new job to worker
	for i := range numJobs {
		if exists := c.finishedJobs[i]; !exists {
			if exists := c.pendingJobs[i]; !exists {
				reply.JobId = i
				c.pendingJobs[i] = true
				c.pendingWorkers[resource.WorkerId] = PendingJob{jobId: i, startTime: int(time.Now().Unix())}
				c.assignInputFiles(reply)
				return
			}
		}
	}

	// reassign stuck job to worker
	for workerId, job := range c.pendingWorkers {
		if int(time.Now().Unix())-job.startTime > 10 {
			delete(c.pendingWorkers, workerId)
			reply.JobId = job.jobId
			c.pendingWorkers[resource.WorkerId] = PendingJob{job.jobId, int(time.Now().Unix())}
			c.assignInputFiles(reply)
			return
		}
	}

	reply.Job = Wait
}

func (c *Coordinator) assignInputFiles(reply *NotifyReply) {
	switch reply.Job {
	case Map:
		reply.InputFileForMapJob = c.inputFiles[reply.JobId]
	case Reduce:
		reply.InputFilesForReduceJob = c.outputFilesFromMapJobs[reply.JobId]
	}
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.outputFilesFromReduceJobs) == c.numReduceJobs {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce Jobs to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	fmt.Printf("starting coordinator\n")

	c.numReduceJobs = nReduce
	c.inputFiles = files

	c.pendingWorkers = make(map[int]PendingJob)
	c.pendingJobs = make(map[int]bool)
	c.finishedJobs = make(map[int]bool)

	c.numFinishedMapJobs = 0
	c.outputFilesFromMapJobs = make([][]string, nReduce)
	for i := 0; i < nReduce; i++ {
		c.outputFilesFromMapJobs[i] = make([]string, 0)
	}
	c.outputFilesFromReduceJobs = make(map[string]bool)

	c.server()
	return &c
}
