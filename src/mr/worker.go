package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// Job number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := os.Getpid()
	log.Printf("starting worker with id %d\n", workerId)

	workerResource := NotifyArgs{}
	workerResource.WorkerId = workerId
	workerResource.Job = Wait

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for workerResource.Job != Done {
		//time.Sleep(5 * time.Second)
		coordinatorMsg, ok := callNotify(workerResource)
		if ok != true {
			log.Printf("fail to call coordinator, done")
			workerResource.Job = Done
		} else {
			workerResource = executeJob(workerResource, coordinatorMsg, mapf, reducef)
		}

	}
}

func callNotify(args NotifyArgs) (NotifyReply, bool) {
	reply := NotifyReply{}
	ok := call("Coordinator.Notify", &args, &reply)
	return reply, ok
}

func executeJob(resource NotifyArgs, coordinatorMsg NotifyReply, mapf func(string, string) []KeyValue, reducef func(string, []string) string) NotifyArgs {
	newResource := NotifyArgs{}
	newResource.WorkerId = resource.WorkerId
	newResource.Job = coordinatorMsg.Job
	switch newResource.Job {
	case Map:
		log.Printf("executing mapper Job\n")
		executeMap(coordinatorMsg, mapf, &newResource)
	case Reduce:
		log.Printf("executing reduce Job\n")
		executeReduce(coordinatorMsg, reducef, &newResource)
	case Wait:
		log.Printf("wait\n")
		time.Sleep(1 * time.Second)
	case Done:
		log.Printf("done\n")
	}
	return newResource
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func executeReduce(coordinatorMsg NotifyReply, reducef func(string, []string) string, newResource *NotifyArgs) {
	intermediate := make([]KeyValue, 0)
	for _, fileName := range coordinatorMsg.InputFilesForReduceJob {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		fileScanner := bufio.NewScanner(file)
		fileScanner.Split(bufio.ScanLines)
		for fileScanner.Scan() {
			words := strings.Fields(fileScanner.Text())
			intermediate = append(intermediate, KeyValue{words[0], words[1]})
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	outputName := "mr-out-reduce-" + strconv.Itoa(coordinatorMsg.JobId) + "-" + strconv.Itoa(newResource.WorkerId)
	file, _ := os.Create(outputName)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	newResource.ReduceJobOutputFile = outputName
}

func executeMap(coordinatorMsg NotifyReply, mapf func(string, string) []KeyValue, newResource *NotifyArgs) {
	newResource.MapJobOutputFiles = make([]string, coordinatorMsg.NumReduceJobs)
	intermediate := make([][]KeyValue, coordinatorMsg.NumReduceJobs)

	content := readFile(coordinatorMsg.InputFileForMapJob)
	kva := mapf(coordinatorMsg.InputFileForMapJob, content)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % coordinatorMsg.NumReduceJobs
		intermediate[reduceId] = append(intermediate[reduceId], kv)
	}

	count := 0
	for i, kva := range intermediate {
		outputName := "mr-out-map-" + strconv.Itoa(coordinatorMsg.JobId) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(newResource.WorkerId)
		newResource.MapJobOutputFiles[i] = outputName
		file, _ := os.Create(outputName)
		for _, kv := range kva {
			fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
			count += 1
		}
	}
	fmt.Printf("number of words: %d \n", count)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func readFile(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
