package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type client struct {
	counter int
	value   string
}
type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data    map[string]string
	clients map[int64]client
}

func (kv *KVServer) dedup(id int64, counter int) (string, bool) {
	if _, exists := kv.clients[id]; !exists {
		kv.clients[id] = client{-1, ""}
	}
	if counter <= kv.clients[id].counter {
		return kv.clients[id].value, true
	}
	return "", false
}

func (kv *KVServer) store(id int64, counter int, value string, save bool) {
	if save {
		kv.clients[id] = client{counter, value}
	} else {
		kv.clients[id] = client{counter, ""}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, _ := kv.data[args.Key]
	reply.Value = v
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, dup := kv.dedup(args.ClientId, args.ClientCounter)
	if dup {
		return
	}

	kv.data[args.Key] = args.Value
	kv.store(args.ClientId, args.ClientCounter, "", false)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, dup := kv.dedup(args.ClientId, args.ClientCounter)
	if dup {
		reply.Value = v
		return
	}

	v, _ = kv.data[args.Key]
	kv.data[args.Key] = v + args.Value
	reply.Value = v
	kv.store(args.ClientId, args.ClientCounter, v, true)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.clients = make(map[int64]client)
	return kv
}
