// Package gopaxos implements a simple paxos algorithm for learning purpose.
package gopaxos

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

// proposer(v):
//   while not decided:
//     choose n, unique and higher than any n seen so far
//     send prepare(n) to all servers including self
//     if prepare_ok(n_a, v_a) from majority:
//       v' = v_a with highest n_a; choose own v otherwise
//       send accept(n, v') to all
//       if accept_ok(n) from majority:
//         send decided(v') to all
//
// acceptor's state:
//   n_p (highest prepare seen)
//   n_a, v_a (highest accept seen)
//
// acceptor's prepare(n) handler:
//   if n > n_p
//     n_p = n
//     reply prepare_ok(n_a, v_a)
//   else
//     reply prepare_reject
//
// acceptor's accept(n, v) handler:
//   if n >= n_p
//     n_p = n
//     n_a = n
//     v_a = v
//     reply accept_ok(n)
//   else
//     reply accept_reject

type commitLog struct {
	data map[int]Value
}

func (cl *commitLog) Write(seq int, v Value) {
	cl.data[seq] = v
}

func (cl *commitLog) Get(seq int) (Value, bool) {
	v, ok := cl.data[seq]
	return v, ok
}

type Paxos struct {
	id            int
	peers         []string
	unreliableRPC bool
	rpcCount      int
	logger        *commitLog
	mu            sync.Mutex

	// state
	minSeq int
	maxSeq int
}

type Request struct {
	FromID int
	Seq    int
}

type Response struct {
}

type Value interface{}

// RPCs
type Handler struct {
	pxs *Paxos
}

func NewHandler(pxs *Paxos) *Handler {
	return &Handler{pxs: pxs}
}

func (h *Handler) OnReceiveProposal(req *Request, response *Response) error {
	fmt.Printf("Instance: %d, OnReceiveProposal: %+v\n", h.pxs.ID(), req)
	return nil
}

func (h *Handler) OnReceiveAcceptance(req *Request, response *Response) error {
	fmt.Printf("Instance: %d, OnReceiveAcceptance: %+v\n", h.pxs.ID(), req)
	return nil
}

func Make(peers []string, id int) *Paxos {
	if id < 0 || id >= len(peers) {
		panic(fmt.Sprintf("invalid set up, peers: %v, id: %d", peers, id))
	}
	pxs := &Paxos{
		id:            id,
		peers:         peers,
		unreliableRPC: false,
		logger:        &commitLog{data: make(map[int]Value)},
	}

	server := rpc.NewServer()
	server.Register(NewHandler(pxs))
	addrAndPath := strings.Split(peers[id], "/")
	if len(addrAndPath) != 2 {
		panic(fmt.Sprintf("got: %v, want: ${HOSTNAME}:${PORT}/${RPC_PATH}", addrAndPath))
	}
	addr := addrAndPath[0]
	rpcPath := addrAndPath[1]

	http.Handle(rpcPath, server)

	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(listen, nil)

	return pxs
}

// Start starts an agreement on new instance.
func (p *Paxos) Start(seq int, v Value) {
	var clients []*rpc.Client
	for _, peer := range p.peers {
		addrAndPath := strings.Split(peer, "/")
		if len(addrAndPath) != 2 {
			panic(fmt.Sprintf("got: %v, want: ${HOSTNAME}:${PORT}/${RPC_PATH}", addrAndPath))
		}
		addr := addrAndPath[0]
		rpcPath := addrAndPath[1]
		client, err := rpc.DialHTTPPath("tcp", addr, rpcPath)
		if err == nil {
			clients = append(clients, client)
		} else {
			fmt.Printf("Error: %v\n", err)

			clients = append(clients, nil)
		}
	}

	p.maxSeq++
	for _, c := range clients {
		if c != nil {
			c.Go("Handler.OnReceiveProposal",
				&Request{
					FromID: p.ID(),
					Seq:    p.Max(),
				},
				&Response{}, nil)
		}
	}
}

// // Status gets info about an instance.
func (p *Paxos) Status(seq int) (bool, Value) {
	v, ok := p.logger.Get(seq)
	return ok, v
}

// Done means it is ok to forget all instances <= seq
func (p *Paxos) Done(seq int) {}

// Max returns the highest instance seq known, or -1.
func (p *Paxos) Max() int {
	return p.maxSeq
}

// instances before this have been forgotten
func (p *Paxos) Min() int {
	return p.minSeq
}

func (p *Paxos) Kill() {
}

func (p *Paxos) ID() int {
	return p.id
}

func (p *Paxos) EnableUnReliableRPC() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unreliableRPC = true
}

func (p *Paxos) EnableReliableRPC() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unreliableRPC = false
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
