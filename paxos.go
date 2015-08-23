// Package gopaxos implements a simple paxos algorithm for learning purpose.
package gopaxos

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

type Paxos struct {
	id         int
	peers      []string
	unreliable bool
	rpcCount   int
}

type Value interface{}

func Make(peers []string, me int) *Paxos {
	return &Paxos{
		id:    me,
		peers: peers,
	}
}

// Start starts an agreement on new instance.
func (p *Paxos) Start(seq int, v Value) {}

// // Status gets info about an instance.
func (p *Paxos) Status(seq int) (bool, Value) {
	return false, nil
}

// Done means it is ok to forget all instances <= seq
func (p *Paxos) Done(seq int) {}

// Max returns the highest instance seq known, or -1.
func (p *Paxos) Max() int {
	return 0
}

// instances before this have been forgotten
func (p *Paxos) Min() int {
	return 0
}

func (p *Paxos) Kill() {}

func (p *Paxos) ID() int {
	return p.id
}
