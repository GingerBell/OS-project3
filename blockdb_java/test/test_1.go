package main

import (
	"encoding/json"
	"fmt"
	"time"
	"sync"
	"os/exec"
	"math/rand"
	"io/ioutil"
	"log"
	"sync/atomic"

	pb "../protobuf/go"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var address = func() string {
	conf, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}
	var dat map[string]interface{}
	err = json.Unmarshal(conf, &dat)
	if err != nil {
		panic(err)
	}
	dat = dat["1"].(map[string]interface{})
	return fmt.Sprintf("%s:%s", dat["ip"], dat["port"])
}()
var wait sync.WaitGroup

func getc() (*grpc.ClientConn, pb.BlockDatabaseClient) {
	for {
		if conn, err := grpc.Dial(address, grpc.WithInsecure()); err == nil {
			c := pb.NewBlockDatabaseClient(conn)
			return conn, c
		}
	}
}

func get(id string) int32 {
	conn, c := getc()
	defer conn.Close()
	if r, err := c.Get(context.Background(), &pb.GetRequest{UserID: id}); err != nil {
		log.Printf("GET Error: %v", err)
		time.Sleep(500 * time.Millisecond)
		return -1
	} else {
		log.Printf("GET %s: %d", id, r.Value)
		return r.Value
	}
}

func put(id string, value int32) bool {
	conn, c := getc()
	defer conn.Close()
	if r, err := c.Put(context.Background(), &pb.Request{UserID: id, Value: value}); err != nil {
		log.Printf("PUT Error: %v", err)
		time.Sleep(500 * time.Millisecond)
		return false
	} else {
		log.Printf("PUT %s %d: %t", id, value, r.Success)
		return r.Success
	}
}

func deposit(id string, value int32) bool {
	conn, c := getc()
	defer conn.Close()
	if r, err := c.Deposit(context.Background(), &pb.Request{UserID: id, Value: value}); err != nil {
		log.Printf("DEPOSIT Error: %v", err)
		time.Sleep(500 * time.Millisecond)
		return false
	} else {
		return r.Success
	}
}

func withdraw(id string, value int32) bool {
	conn, c := getc()
	defer conn.Close()
	if r, err := c.Withdraw(context.Background(), &pb.Request{UserID: id, Value: value}); err != nil {
		log.Printf("WITHDRAW Error: %v", err)
		time.Sleep(500 * time.Millisecond)
		return false
	} else {
		return r.Success
	}
}

func transfer(fromId, toId string, value int32) bool {
	conn, c := getc()
	defer conn.Close()
	if r, err := c.Transfer(context.Background(), &pb.TransferRequest{FromID: fromId, ToID: toId, Value: value}); err != nil {
		log.Printf("TRANSFER Error: %v", err)
		time.Sleep(500 * time.Millisecond)
		return false
	} else {
		log.Printf("TRANSFER %s %s %d: %t", fromId, toId, value, r.Success)
		return r.Success
	}
}

func logLength() int32 {
	conn, c := getc()
	defer conn.Close()
	if r, err := c.LogLength(context.Background(), &pb.Null{}); err != nil {
		log.Printf("LogLength Error: %v", err)
		time.Sleep(500 * time.Millisecond)
		return -1
	} else {
		return r.Value
	}
}

func startAndKill(stop *int32) {
	defer wait.Done()

	for atomic.LoadInt32(stop) == 0 {
		log.Printf("start")

		cmd := exec.Command("test/server_start.sh")
		pid, err := cmd.Output()
		if err != nil {
			log.Fatalf("Cannot start the server: %v", err)
			return
		}

		time.Sleep(2000 * time.Millisecond)

		log.Printf("kill %s", pid)

		cmd2 := exec.Command("kill", string(pid))
		if err := cmd2.Run(); err != nil {
			log.Fatalf("Cannot kill the server: %v", err)
			return
		}
	}
}

func main() {
	var stop int32 = 0

	wait.Add(1)

	go startAndKill(&stop)

	rand.Seed(time.Now().UTC().UnixNano())

	const N = 10
	const T = 10000
	const M = 10000

	var names [N] string

	for i := 0; i < N; i++ {
		names[i] = fmt.Sprintf("user_%d", rand.Int63())
	}

	for i := 0; i < N; i++ {
		for !put(names[i], M) { }
	}

	for t := 0; t < T; t++ {
		from := names[rand.Int31n(N)]
		to := names[rand.Int31n(N)]
		m := rand.Int31n(100) + 1
		transfer(from, to, m)

		if t % 100 == 0 {
			log.Printf("time: %d", t)
		}
	}

	var total = int32(0)
	for i := 0; i < N; i++ {
		for {
			m := get(names[i])
			if m != -1 {
				total += m
				break
			}
		}
	}

	log.Printf("%d (expected: %d)", total, N * M)

	atomic.StoreInt32(&stop, 1)

	wait.Wait()
}
