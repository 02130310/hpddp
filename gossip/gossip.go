package main

import (
    "encoding/json"
    "fmt"
    "gitlab.com/n-canter/graph"
    "math/rand"
    "net"
    "os"
    "sync"
    "time"
)

type Message struct {
    Id int
    Type string
    Sender int
    Origin int
    Data string
}

func is_ok (err error) {
    if err != nil {
        panic(err)
    }
}

func send (message *Message, port int) {
    ServerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("127.0.0.1:%v",
                                                    port))
    is_ok(err)
    LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
    is_ok(err)
    Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
    is_ok(err)

    b, err := json.Marshal(*message)
    is_ok(err)
    _, err = Conn.Write(b)
    is_ok(err)
    Conn.Close()
}

func gossip (m *Message, g *graph.Graph, my_id int, excluded int, base_port int,
             ttl int, t time.Duration) {
    nb, _ := g.Neighbors(my_id)
    for i, node := range nb {
        if node.Port() == (base_port + excluded) {
            nb[len(nb)-1], nb[i] = nb[i], nb[len(nb)-1]
            break
        }
    }
    nb = nb[:len(nb)-1]

    for ; ttl >= 0; ttl-- {
        neighbor_id := rand.Intn(len(nb))
        send(m, nb[neighbor_id].Port())
        time.Sleep(t)
    }
}

func routine (my_id int, nodes int, base_port int, ttl int, t time.Duration,
              g *graph.Graph) {
    var seen_confirmations = make(map[int]bool)
    seen_message := false
    var confirmations_left = nodes // for node 0
    var msg_id = my_id * 100000

    node, _ := g.GetNode(my_id)
    port := node.Port()
    udp_addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%v", port))
    is_ok(err)
    conn, err := net.ListenUDP("udp", udp_addr)
    is_ok(err)
    defer conn.Close()

    var start time.Time
    if my_id == 0 { // send the initial message
        var m = Message { msg_id, "multicast", my_id, my_id, "message" }
        msg_id += 1
        send(&m, base_port)
        start = time.Now()
    }

    for {
        buf := make([]byte, 1024)
        n, _, err := conn.ReadFromUDP(buf)
        is_ok(err)
        var m Message
        err = json.Unmarshal(buf[:n], &m)
        is_ok(err)
        if m.Type == "notification" {
            _, ok := seen_confirmations[m.Origin]
            if !ok {
                // a new confirmation
                seen_confirmations[m.Origin] = true
                confirmations_left -= 1
                //fmt.Printf("node %v, recv new confirmation from %v, left %v\n",
                //           my_id, m.Origin, confirmations_left)
                m.Sender = my_id
                m.Id = msg_id
                msg_id += 1
                go gossip(&m, g, my_id, m.Origin, base_port, ttl, t)
                if (my_id == 0) && (confirmations_left == 0) {
                    fmt.Printf("%v\n",
                               float64(time.Since(start).Nanoseconds()) /
                               float64(t.Nanoseconds()))
                    os.Exit(0)
                }
            }
        } else {
            if !seen_message {
                seen_message = true
                //fmt.Printf("node %v, recv msg from %v\n", my_id, m.Origin)
                m.Sender = my_id
                m.Id = msg_id
                msg_id += 1
                go gossip(&m, g, my_id, m.Origin, base_port, ttl, t)
                var my_confirmation = Message { msg_id, "notification", my_id, my_id, ""}
                msg_id += 1
                go gossip(&my_confirmation, g, my_id, m.Origin, base_port, ttl, t)
            }
        }
    }
}

func main () {
    nodes := 10
    base_port := 30000
    ttl := 32
    t := time.Millisecond * time.Duration(5)
    rand.Seed(time.Now().UnixNano())
    g := graph.Generate(nodes, 3, 8, base_port)

    var wg sync.WaitGroup
    for i := 0; i < nodes; i++ {
        wg.Add(1)
        go routine(i, nodes, base_port, ttl, t, &g)
    }

    wg.Wait()
}
