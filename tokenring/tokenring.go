package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "net"
    "sync"
    "time"
)

func is_ok (err error) {
    if err != nil {
        panic(err)
    }
}

const DataPort = 30000
const MaintPort = 40000

type MaintMessage struct {
    Msgtype string `json:"type"` // "send" or "drop"
    Msgdst int `json:"dst"` // if "send"
    Msgdata string `json:"data"` // if "send"
}

type Token struct {
    Msgtype string // "data", "confirm" or "empty"
    Msgdest int // if "data" or "confirm"
    Msgsrc int // if "data" or "confirm"
    Msgdata string // if "data"
    From int
    To int
}

func listener (conn *net.UDPConn, c chan []byte, wg *sync.WaitGroup) {
    defer wg.Done()
    for {
        buf := make([]byte, 1024)
        n, _, err := conn.ReadFromUDP(buf)
        is_ok(err)
        if n > 0 {
            c <- buf[0:n]
        }
    }
}

func set_up_udp_conn (port int) *net.UDPConn {
    udp_addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%v", port))
    is_ok(err)
    data_udp_conn, err := net.ListenUDP("udp", udp_addr)
    is_ok(err)
    return data_udp_conn
}

func send_token (token *Token, from int, port int) {
    ServerAddr, err := net.ResolveUDPAddr("udp",fmt.Sprintf("127.0.0.1:%v",
                                                            DataPort + port))
    is_ok(err)
    LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
    is_ok(err)
    Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
    is_ok(err)
    defer Conn.Close()

    token.From = from
    token.To = port
    b, err := json.Marshal(*token)
    is_ok(err)
    _,err = Conn.Write(b)
    is_ok(err)
}

func routine (my_id int, d time.Duration, routines int, wg *sync.WaitGroup) {
    defer wg.Done()

    data_udp_conn := set_up_udp_conn(DataPort + my_id)
    defer data_udp_conn.Close()
    maint_udp_conn := set_up_udp_conn(MaintPort + my_id)
    defer maint_udp_conn.Close()

    var sub_wg sync.WaitGroup
    data_channel := make(chan []byte)
    go listener(data_udp_conn, data_channel, &sub_wg)
    maint_channel := make(chan []byte)
    go listener(maint_udp_conn, maint_channel, &sub_wg)
    sub_wg.Add(2)
    defer sub_wg.Wait()

    drop := false // drop the next token?
    have_msg := false // do we have anything to send?
    confirm_wait := false // are we waiting for a confirmation?
    var confirm_node int // if yes, from whom?

    var token, prepared_token Token // token recieved and token to send
    held_token_timer := time.NewTimer(0) // for when we hold the token
    if !held_token_timer.Stop() {
	    <-held_token_timer.C
    }
    next_token_timer := time.NewTimer(0) // for when we wait for the token
    if !next_token_timer.Stop() {
	    <-next_token_timer.C
    }

    for {
        select {
        case buf := <-data_channel:
            held_token_timer.Reset(d)
            next_token_timer.Stop()
            err := json.Unmarshal(buf, &token)
            is_ok(err)

            // we have to perform three checks, in order:
            // 1) are we waiting for a confirmation? is it in this token?
            // 2) does the token contain a message for us?
            // 3) do we want to send a message?
            //    is this token empty?
            if confirm_wait &&
               token.Msgtype == "confirm" &&
               token.Msgsrc == confirm_node &&
               token.Msgdest == my_id {
                confirm_wait = false
                prepared_token.Msgtype = "empty"
                // not really needed:
                prepared_token.Msgdest = 0
                prepared_token.Msgsrc = 0
                prepared_token.Msgdata = ""

            } else if token.Msgtype == "data" &&
                      token.Msgdest == my_id {
                //fmt.Printf()
                prepared_token.Msgtype = "confirm"
                prepared_token.Msgdest = token.Msgsrc
                prepared_token.Msgsrc = my_id
                prepared_token.Msgdata = ""

            } else if have_msg && token.Msgtype == "empty" {
                // our message is already in prepared_token
                have_msg = false
                confirm_wait = true

            } else {
                prepared_token = token
                // send as is
            }

        case buf := <-maint_channel:
            var m MaintMessage
            err := json.Unmarshal(buf, &m)
            is_ok(err)
            fmt.Printf("node %v: recieved service message: %v\n",
                       my_id, string(buf))
            switch m.Msgtype {
            case "send":
                prepared_token.Msgtype = "data"
                prepared_token.Msgdata = m.Msgdata
                prepared_token.Msgdest = m.Msgdst
                prepared_token.Msgsrc = my_id
                have_msg = true
                confirm_node = m.Msgdst
            case "drop":
                drop = true
            }
        case <-next_token_timer.C:
            fmt.Printf("node %v: token lost, ", my_id)
            // token lost
            prepared_token.Msgtype = "empty"
            prepared_token.Msgdest = 0
            prepared_token.Msgsrc = 0
            prepared_token.Msgdata = ""
            if drop {
                drop = false;
            } else {
                fmt.Printf("sending token to node %v\n", (my_id + 1) % routines)
                send_token(&prepared_token, my_id, (my_id + 1) % routines)
            }
            next_token_timer.Reset((d) * time.Duration(routines * 2))
        case <-held_token_timer.C:
            // send token
            fmt.Printf("node %v: recv token from %v", my_id, token.From)
            switch token.Msgtype {
            case "empty":
                fmt.Printf(", ")
            case "data":
                fmt.Printf(" with data <%v> from %v for %v, ",
                            token.Msgdata, token.Msgsrc, token.Msgdest)
            case "confirm":
                fmt.Printf(" with delivery confirmation from %v for %v, ",
                            token.Msgsrc, token.Msgdest)
            }
            fmt.Printf("sending token to node %v\n", (my_id + 1) % routines)

            if drop {
                drop = false;
            } else {
                send_token(&prepared_token, my_id, (my_id + 1) % routines)
            }
            next_token_timer.Reset((d) * time.Duration(routines * 2))
        }
    }
}


func main () {
    routines_ptr := flag.Int("n", 10, "number of nodes")
    delta_ptr := flag.Int("t", 500, "duration in milliseconds to hold tokens for")
    flag.Parse()

    var routines int = *routines_ptr
    var d = time.Duration(*delta_ptr) * time.Millisecond

    var wg sync.WaitGroup
    for i := 0; i < routines; i++ {
        wg.Add(1)
        go routine(i, d, routines, &wg)
    }

    var prepared_token = Token { "empty", 0, 0, "", 0, 0 }
    time.Sleep(time.Duration(2) * d)
    send_token(&prepared_token, -1, 0)

    wg.Wait()
}
