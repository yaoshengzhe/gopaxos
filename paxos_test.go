package gopaxos

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/golang/glog"
)

func TestMain(m *testing.M) {
	runtime.GOMAXPROCS(4)
	os.Exit(m.Run())
}

func TestGoPaxosSingleProposer(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("single-proposer", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: Single proposer ...")

	pxa[0].Start(0, "hello")
	if err := waitN(pxa, 0, npaxos); err != nil {
		t.Fatal(err)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosManyProposersSameValue(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("many-proposers-same-value", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: Many proposers, same value ...")

	for i := 0; i < npaxos; i++ {
		pxa[i].Start(1, 77)
	}
	if err := waitN(pxa, 1, npaxos); err != nil {
		t.Fatal(err)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosManyProposersDifferentValues(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("many-proposers-different-values", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: Many proposers, different values ...")

	pxa[0].Start(2, 100)
	pxa[1].Start(2, 101)
	pxa[2].Start(2, 102)

	if err := waitN(pxa, 2, npaxos); err != nil {
		t.Fatal(err)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosOutOfOrderInstances(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("out-of-order-instances", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: Out-of-order instances ...")

	pxa[0].Start(7, 700)
	pxa[0].Start(6, 600)
	pxa[1].Start(5, 500)

	if err := waitN(pxa, 7, npaxos); err != nil {
		t.Fatal(err)
	}

	pxa[0].Start(4, 400)
	pxa[1].Start(3, 300)

	if err := waitN(pxa, 6, npaxos); err != nil {
		t.Fatal(err)
	}
	if err := waitN(pxa, 5, npaxos); err != nil {
		t.Fatal(err)
	}
	if err := waitN(pxa, 4, npaxos); err != nil {
		t.Fatal(err)
	}
	if err := waitN(pxa, 3, npaxos); err != nil {
		t.Fatal(err)
	}

	if pxa[0].Max() != 7 {
		t.Fatalf("pxa[0].Max() = %d, want: 7", pxa[0].Max())
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosDeafProposer(t *testing.T) {
	npaxos := 5
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("deaf-proposer", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: Deaf proposer ...")

	pxa[0].Start(0, "hello")
	if err := waitN(pxa, 0, npaxos); err != nil {
		t.Fatal(err)
	}
	// TODO os.Remove(pxh[0])
	// TODO os.Remove(pxh[npaxos-1])
	pxa[1].Start(1, "goodbye")
	if err := waitMajority(pxa, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	count, err := ndecided(pxa, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != npaxos-2 {
		t.Fatal("a deaf peer heard about a decision")
	}

	pxa[0].Start(1, "xxx")
	if err := waitN(pxa, 1, npaxos-1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)
	count, err = ndecided(pxa, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count != npaxos-1 {
		t.Fatal("a deaf peer heard about a decision")
	}

	pxa[npaxos-1].Start(1, "yyy")
	if err := waitN(pxa, 1, npaxos); err != nil {
		t.Fatal(err)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosForgetting(t *testing.T) {
	npaxos := 6
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("forgetting", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: Forgetting ...")

	// initial Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m > 0 {
			t.Fatalf("wrong initial Min() %v", m)
		}
	}

	pxa[0].Start(0, "00")
	pxa[1].Start(1, "11")
	pxa[2].Start(2, "22")
	pxa[0].Start(6, "66")
	pxa[1].Start(7, "77")

	if err := waitN(pxa, 0, npaxos); err != nil {
		t.Fatal(err)
	}

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	if err := waitN(pxa, 1, npaxos); err != nil {
		t.Fatal(err)
	}

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := pxa[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	// everyone Done() -> Min() changes?
	for i := 0; i < npaxos; i++ {
		pxa[i].Done(0)
	}
	for i := 1; i < npaxos; i++ {
		pxa[i].Done(1)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i].Start(8+i, "xx")
	}
	allok := false
	for iters := 0; iters < 12; iters++ {
		allok = true
		for i := 0; i < npaxos; i++ {
			s := pxa[i].Min()
			if s != 1 {
				allok = false
			}
		}
		if allok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if allok != true {
		t.Fatalf("Min() did not advance after Done()")
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosLotsOfForgetting(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("lots-of-forgetting", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
		pxa[i].EnableUnReliableRPC()
	}

	fmt.Println("Test: Lots of forgetting ...")

	const maxseq = 20
	done := false

	go func() {
		na := rand.Perm(maxseq)
		for i := 0; i < len(na); i++ {
			seq := na[i]
			j := (rand.Int() % npaxos)
			v := rand.Int()
			pxa[j].Start(seq, v)
			runtime.Gosched()
		}
	}()

	go func() {
		for done == false {
			seq := (rand.Int() % maxseq)
			i := (rand.Int() % npaxos)
			if seq >= pxa[i].Min() {
				decided, _ := pxa[i].Status(seq)
				if decided {
					pxa[i].Done(seq)
				}
			}
			runtime.Gosched()
		}
	}()

	time.Sleep(5 * time.Second)
	done = true
	for i := 0; i < npaxos; i++ {
		pxa[i].EnableReliableRPC()
	}
	time.Sleep(2 * time.Second)

	for seq := 0; seq < maxseq; seq++ {
		for i := 0; i < npaxos; i++ {
			if seq >= pxa[i].Min() {
				pxa[i].Status(seq)
			}
		}
	}

	fmt.Println("  ... Passed")
}

//
// does paxos forgetting actually free the memory?
//
func TestGoPaxosFreesForgottenInstanceMemory(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("frees-forgotten-instance-memory", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: Paxos frees forgotten instance memory ...")

	pxa[0].Start(0, "x")
	if err := waitN(pxa, 0, npaxos); err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// m0.Alloc about a megabyte

	for i := 1; i <= 10; i++ {
		big := make([]byte, 1000000)
		for j := 0; j < len(big); j++ {
			big[j] = byte('a' + rand.Int()%26)
		}
		pxa[0].Start(i, string(big))
		if err := waitN(pxa, i, npaxos); err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	// m1.Alloc about 90 megabytes

	for i := 0; i < npaxos; i++ {
		pxa[i].Done(10)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i].Start(11+i, "z")
	}
	time.Sleep(3 * time.Second)
	for i := 0; i < npaxos; i++ {
		if pxa[i].Min() != 11 {
			t.Fatalf("expected Min() %v, got %v", 11, pxa[i].Min())
		}
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	// m2.Alloc about 10 megabytes

	if m2.Alloc > (m1.Alloc / 2) {
		t.Fatalf("memory use did not shrink enough")
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosRPCCountArentTooHigh(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("rpc-count-arent-too-high", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	fmt.Println("Test: RPC counts aren't too high ...")

	ninst1 := 5
	seq := 0
	for i := 0; i < ninst1; i++ {
		pxa[0].Start(seq, "x")
		if err := waitN(pxa, seq, npaxos); err != nil {
			t.Fatal(err)
		}
		seq++
	}

	time.Sleep(2 * time.Second)

	total1 := 0
	for j := 0; j < npaxos; j++ {
		total1 += pxa[j].rpcCount
	}

	// per agreement:
	// 3 prepares
	// 3 accepts
	// 3 decides
	expected1 := ninst1 * npaxos * npaxos
	if total1 > expected1 {
		t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
			ninst1, total1, expected1)
	}

	ninst2 := 5
	for i := 0; i < ninst2; i++ {
		for j := 0; j < npaxos; j++ {
			go pxa[j].Start(seq, j+(i*10))
		}
		if err := waitN(pxa, seq, npaxos); err != nil {
			t.Fatal(err)
		}

		seq++
	}

	time.Sleep(2 * time.Second)

	total2 := 0
	for j := 0; j < npaxos; j++ {
		total2 += pxa[j].rpcCount
	}
	total2 -= total1

	// worst case per agreement:
	// Proposer 1: 3 prep, 3 acc, 3 decides.
	// Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	// Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	expected2 := ninst2 * npaxos * 15
	if total2 > expected2 {
		t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
			ninst2, total2, expected2)
	}

	fmt.Println("  ... Passed")
}

//
// many agreements (without failures)
//
func TestGoPaxosManyInstances(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("many-instances", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
		pxa[i].Start(0, 0)
	}

	fmt.Println("Test: Many instances ...")

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		if seq >= 5 {
			// only 5 active instances, to limit the
			// number of file descriptors.
			for {
				count, err := ndecided(pxa, seq-5)
				if err != nil {
					t.Fatal(err)
				}
				if count < npaxos {
					time.Sleep(20 * time.Millisecond)
				}
			}
		}
		for i := 0; i < npaxos; i++ {
			pxa[i].Start(seq, (seq*10)+i)
		}
	}

	for {
		done := true
		for seq := 1; seq < ninst; seq++ {
			count, err := ndecided(pxa, seq)
			if err != nil {
				t.Fatal(err)
			}
			if count < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("  ... Passed")
}

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
//
func TestGoPaxosMinorityProposalIgnored(t *testing.T) {
	npaxos := 5
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("minority-proposal-ignored", i)
	}

	pxa[1] = Make(pxh, 1)
	pxa[2] = Make(pxh, 2)
	pxa[3] = Make(pxh, 3)

	fmt.Println("Test: Minority proposal ignored ...")

	pxa[1].Start(1, 111)

	if err := waitMajority(pxa, 1); err != nil {
		t.Fatal(err)
	}

	pxa[0] = Make(pxh, 0)
	pxa[0].Start(1, 222)

	if err := waitN(pxa, 1, 4); err != nil {
		t.Fatal(err)
	}

	if false {
		pxa[4] = Make(pxh, 4)
		if err := waitN(pxa, 1, npaxos); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("  ... Passed")
}

//
// many agreements, with unreliable RPC
//
func TestGoPaxosManyInstancesUnreliableRPC(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("many-instances-unreliable-rpc", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
		pxa[i].EnableUnReliableRPC()
		pxa[i].Start(0, 0)
	}

	fmt.Println("Test: Many instances, unreliable RPC ...")

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		// only 3 active instances, to limit the
		// number of file descriptors.
		if seq >= 3 {
			for {
				count, err := ndecided(pxa, seq-3)
				if err != nil {
					t.Fatal(err)
				}
				if count < npaxos {
					time.Sleep(20 * time.Millisecond)
				}
			}
		}
		for i := 0; i < npaxos; i++ {
			pxa[i].Start(seq, (seq*10)+i)
		}
	}

	for {
		done := true
		for seq := 1; seq < ninst; seq++ {
			count, err := ndecided(pxa, seq)
			if err != nil {
				t.Fatal(err)
			}
			if count < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosNoDecisionIfPartitioned(t *testing.T) {
	tag := "no-decision-if-partitioned"
	npaxos := 5
	pxa := make([]*Paxos, npaxos)
	defer cleanup(pxa)
	defer cleanPartition(tag, npaxos)

	for i := 0; i < npaxos; i++ {
		pxh := make([]string, npaxos)
		for j := 0; j < npaxos; j++ {
			if j == i {
				pxh[j] = port(tag, i)
			} else {
				pxh[j] = partitionedPort(tag, i, j)
			}
		}
		pxa[i] = Make(pxh, i)
	}
	defer func(t *testing.T, tag string, npaxos int) {
		if err := makePartition(tag, npaxos, []int{}, []int{}, []int{}); err != nil {
			t.Fatal(err)
		}
	}(t, tag, npaxos)

	seq := 0

	fmt.Println("Test: No decision if partitioned ...")

	if err := makePartition(tag, npaxos, []int{0, 2}, []int{1, 3}, []int{4}); err != nil {
		t.Fatal(err)
	}
	pxa[1].Start(seq, 111)

	maxNumOfDecided := 0
	count, err := waitInstances(pxa, seq, func(count int) bool {
		time.Sleep(3 * time.Second)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if count > maxNumOfDecided {
		t.Fatalf("too many decided; seq=%v ndecided=%v max decided #instances=%v", seq, count, maxNumOfDecided)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosDecisionInMajorityPartition(t *testing.T) {
	tag := "decision-in-majority-partition"
	npaxos := 5
	pxa := make([]*Paxos, npaxos)
	defer cleanup(pxa)
	defer cleanPartition(tag, npaxos)

	for i := 0; i < npaxos; i++ {
		pxh := make([]string, npaxos)
		for j := 0; j < npaxos; j++ {
			if j == i {
				pxh[j] = port(tag, i)
			} else {
				pxh[j] = partitionedPort(tag, i, j)
			}
		}
		pxa[i] = Make(pxh, i)
	}
	defer func(t *testing.T, tag string, npaxos int) {
		if err := makePartition(tag, npaxos, []int{}, []int{}, []int{}); err != nil {
			t.Fatal(err)
		}
	}(t, tag, npaxos)

	seq := 0

	fmt.Println("Test: Decision in majority partition ...")

	if err := makePartition(tag, npaxos, []int{0}, []int{1, 2, 3}, []int{4}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if err := waitMajority(pxa, seq); err != nil {
		t.Fatal(err)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosAllAgreeAfterFullHeal(t *testing.T) {
	tag := "all-agree-after-full-heal"
	npaxos := 5
	pxa := make([]*Paxos, npaxos)
	defer cleanup(pxa)
	defer cleanPartition(tag, npaxos)

	for i := 0; i < npaxos; i++ {
		pxh := make([]string, npaxos)
		for j := 0; j < npaxos; j++ {
			if j == i {
				pxh[j] = port(tag, i)
			} else {
				pxh[j] = partitionedPort(tag, i, j)
			}
		}
		pxa[i] = Make(pxh, i)
	}
	defer func(t *testing.T, tag string, npaxos int) {
		if err := makePartition(tag, npaxos, []int{}, []int{}, []int{}); err != nil {
			t.Fatal(err)
		}
	}(t, tag, npaxos)

	seq := 0

	fmt.Println("Test: All agree after full heal ...")

	pxa[0].Start(seq, 1000) // poke them
	pxa[4].Start(seq, 1004)
	if err := makePartition(tag, npaxos, []int{0, 1, 2, 3, 4}, []int{}, []int{}); err != nil {
		t.Fatal(err)
	}

	if err := waitN(pxa, seq, npaxos); err != nil {
		t.Fatal(err)
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosOnePeerSwitchesPartitions(t *testing.T) {
	tag := "one-peer-switches-partitions"
	npaxos := 5
	pxa := make([]*Paxos, npaxos)
	defer cleanup(pxa)
	defer cleanPartition(tag, npaxos)

	for i := 0; i < npaxos; i++ {
		pxh := make([]string, npaxos)
		for j := 0; j < npaxos; j++ {
			if j == i {
				pxh[j] = port(tag, i)
			} else {
				pxh[j] = partitionedPort(tag, i, j)
			}
		}
		pxa[i] = Make(pxh, i)
	}
	defer func(t *testing.T, tag string, npaxos int) {
		if err := makePartition(tag, npaxos, []int{}, []int{}, []int{}); err != nil {
			t.Fatal(err)
		}
	}(t, tag, npaxos)

	seq := 0

	fmt.Println("Test: One peer switches partitions ...")

	for iters := 0; iters < 20; iters++ {
		seq++

		if err := makePartition(tag, npaxos, []int{0, 1, 2}, []int{3, 4}, []int{}); err != nil {
			t.Fatal(err)
		}
		pxa[0].Start(seq, seq*10)
		pxa[3].Start(seq, (seq*10)+1)
		if err := waitMajority(pxa, seq); err != nil {
			t.Fatal(err)
		}

		count, err := ndecided(pxa, seq)
		if err != nil {
			t.Fatal(err)
		}
		if count > 3 {
			t.Fatalf("ndecided(pxa, %d) = %d, want: 3", seq, count)
		}

		if err := makePartition(tag, npaxos, []int{0, 1}, []int{2, 3, 4}, []int{}); err != nil {
			t.Fatal(err)
		}
		if err := waitN(pxa, seq, npaxos); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosOnePeerSwitchesPartitionsUnReliable(t *testing.T) {
	tag := "one-peer-switches-partitions-unreliable"
	npaxos := 5
	pxa := make([]*Paxos, npaxos)
	defer cleanup(pxa)
	defer cleanPartition(tag, npaxos)

	for i := 0; i < npaxos; i++ {
		pxh := make([]string, npaxos)
		for j := 0; j < npaxos; j++ {
			if j == i {
				pxh[j] = port(tag, i)
			} else {
				pxh[j] = partitionedPort(tag, i, j)
			}
		}
		pxa[i] = Make(pxh, i)
	}
	defer func(t *testing.T, tag string, npaxos int) {
		if err := makePartition(tag, npaxos, []int{}, []int{}, []int{}); err != nil {
			t.Fatal(err)
		}
	}(t, tag, npaxos)

	seq := 0

	fmt.Println("Test: One peer switches partitions, unreliable ...")

	for iters := 0; iters < 20; iters++ {
		seq++

		for i := 0; i < npaxos; i++ {
			pxa[i].EnableUnReliableRPC()
		}

		if err := makePartition(tag, npaxos, []int{0, 1, 2}, []int{3, 4}, []int{}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < npaxos; i++ {
			pxa[i].Start(seq, (seq*10)+i)
		}
		if err := waitN(pxa, seq, 3); err != nil {
			t.Fatal(err)
		}

		count, err := ndecided(pxa, seq)
		if err != nil {
			t.Fatal(err)
		}
		if count > 3 {
			t.Fatalf("ndecided(pxa, %d) = %d, want: 3", seq, count)
		}

		if err := makePartition(tag, npaxos, []int{0, 1}, []int{2, 3, 4}, []int{}); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < npaxos; i++ {
			pxa[i].EnableReliableRPC()
		}

		if err := waitN(pxa, seq, 5); err != nil {
			t.Fatal(err)
		}

	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosManyRequestsChangingPartitions(t *testing.T) {
	tag := "many-request-changing-partitions"
	const npaxos = 5
	pxa := make([]*Paxos, npaxos)
	defer cleanup(pxa)
	defer cleanPartition(tag, npaxos)

	for i := 0; i < npaxos; i++ {
		pxh := make([]string, npaxos)
		for j := 0; j < npaxos; j++ {
			if j == i {
				pxh[j] = port(tag, i)
			} else {
				pxh[j] = partitionedPort(tag, i, j)
			}
		}
		pxa[i] = Make(pxh, i)
		pxa[i].EnableUnReliableRPC()
	}
	defer makePartition(tag, npaxos, []int{}, []int{}, []int{})

	fmt.Println("Test: Many requests, changing partitions ...")

	done := false

	// re-partition periodically
	ch1 := make(chan bool)
	go func() {
		defer func() { ch1 <- true }()
		for done == false {
			var a [npaxos]int
			for i := 0; i < npaxos; i++ {
				a[i] = (rand.Int() % 3)
			}
			pa := make([][]int, 3)
			for i := 0; i < 3; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < npaxos; j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			if err := makePartition(tag, npaxos, pa[0], pa[1], pa[2]); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	seq := 0

	// periodically start a new instance
	ch2 := make(chan bool)
	go func() {
		defer func() { ch2 <- true }()
		for done == false {
			// how many instances are in progress?
			nd := 0
			for i := 0; i < seq; i++ {
				count, err := ndecided(pxa, i)
				if err != nil {
					t.Fatal(err)
				}
				if count == npaxos {
					nd++
				}
			}
			if seq-nd < 10 {
				for i := 0; i < npaxos; i++ {
					pxa[i].Start(seq, rand.Int()%10)
				}
				seq++
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	// periodically check that decisions are consistent
	ch3 := make(chan bool)
	go func() {
		defer func() { ch3 <- true }()
		for done == false {
			for i := 0; i < seq; i++ {
				if _, err := ndecided(pxa, i); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	time.Sleep(20 * time.Second)
	done = true
	<-ch1
	<-ch2
	<-ch3

	// repair, then check that all instances decided.
	for i := 0; i < npaxos; i++ {
		pxa[i].EnableReliableRPC()
	}
	if err := makePartition(tag, npaxos, []int{0, 1, 2, 3, 4}, []int{}, []int{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)

	for i := 0; i < seq; i++ {
		if err := waitMajority(pxa, i); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println("  ... Passed")
}

func TestGoPaxosConvergenceSpeed(t *testing.T) {
	npaxos := 3
	pxa := make([]*Paxos, npaxos)
	pxh := make([]string, npaxos)
	defer cleanup(pxa)

	for i := 0; i < npaxos; i++ {
		pxh[i] = port("convergence-speed", i)
	}
	for i := 0; i < npaxos; i++ {
		pxa[i] = Make(pxh, i)
	}

	t0 := time.Now()

	for i := 0; i < 20; i++ {
		pxa[0].Start(i, "x")
		if err := waitN(pxa, i, npaxos); err != nil {
			t.Fatal(err)
		}
	}

	d := time.Since(t0)
	fmt.Println("20 agreements %v seconds", d.Seconds())
}

func port(tag string, host int) string {
	/*
		var buf bytes.Buffer
		buf.WriteString("/var/tmp/gopaxos-")
		buf.WriteString(strconv.Itoa(os.Getuid()))
		buf.WriteString("/")

		os.Mkdir(buf.String(), 0777)

		buf.WriteString("px-")
		buf.WriteString(strconv.Itoa(os.Getpid()))
		buf.WriteString("-")
		buf.WriteString(tag)
		buf.WriteString("-")
		buf.WriteString(strconv.Itoa(host))
	*/
	rpcPath := "paxos-" + strconv.Itoa(host) + "-" + tag
	l, _ := net.Listen("tcp", ":0")
	defer l.Close()
	return l.Addr().String() + "/" + rpcPath
}

// ndecided returns #instances that have decided a value in given sequence number.
// It will return an error if they decide on different values.
func ndecided(pxa []*Paxos, seq int) (int, error) {
	var states []struct {
		decided bool
		value   Value
	}
	if testing.Verbose() {
		glog.Infof("Check states of instances, seq = %d", seq)
	}
	for i := 0; i < len(pxa); i++ {
		decided, v := pxa[i].Status(seq)
		states = append(states, struct {
			decided bool
			value   Value
		}{decided, v})
		if testing.Verbose() {
			if decided {
				glog.Infof("instance [%d] has decided a value: %v", pxa[i].ID(), v)
			} else {
				glog.Infof("instance [%d] has not decided a value", pxa[i].ID())
			}
		}
	}

	count := 0
	var curDecidedValue Value

	for i, status := range states {
		if status.decided {
			if count == 0 {
				curDecidedValue = status.value
			} else if curDecidedValue != status.value {
				return -1, fmt.Errorf("decided values do not match; seq=%d i=%v v=%v v1=%v", seq, pxa[i].ID(), curDecidedValue, status.value)
			}
			count++
		}
	}
	return count, nil
}

func waitInstances(pxa []*Paxos, seq int, validateCount func(int) bool) (int, error) {
	defer func(start time.Time) {
		glog.Infof("wait instances took %s", time.Since(start))
	}(time.Now())
	to := 10 * time.Millisecond
	count := -1
	var err error
	for iters := 0; iters < 10; iters++ {
		count, err = ndecided(pxa, seq)
		if err != nil {
			return -1, err
		}
		if validateCount(count) {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
	return count, nil
}

// waitN waits until it finds at least "expectedCount" instances have agreed on the same
// value in given sequence number.
func waitN(pxa []*Paxos, seq int, expectedCount int) error {
	count, err := waitInstances(pxa, seq, func(count int) bool { return count >= expectedCount })
	if err != nil {
		return err
	}
	if count < expectedCount {
		return fmt.Errorf("too few decided; seq=%d decided #instances=%d expect #instances=%d", seq, count, expectedCount)
	}
	return nil
}

func waitMajority(pxa []*Paxos, seq int) error {
	return waitN(pxa, seq, (len(pxa)/2)+1)
}

func cleanup(pxa []*Paxos) {
	for i := 0; i < len(pxa); i++ {
		pxa[i].Kill()
	}
}

func partitionedPort(tag string, src int, dest int) string {
	var buf bytes.Buffer
	buf.WriteString("/var/tmp/gopaxos-")
	buf.WriteString(strconv.Itoa(os.Getuid()))
	buf.WriteString("/px-")
	buf.WriteString(tag)
	buf.WriteString("-")
	buf.WriteString(strconv.Itoa(os.Getpid()))
	buf.WriteString("-")
	buf.WriteString(strconv.Itoa(src))
	buf.WriteString("-")
	buf.WriteString(strconv.Itoa(dest))
	return buf.String()
}

func cleanPartition(tag string, n int) {
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			ij := partitionedPort(tag, i, j)
			os.Remove(ij)
		}
	}
}

func makePartition(tag string, npaxos int, p1 []int, p2 []int, p3 []int) error {
	cleanPartition(tag, npaxos)
	pa := [][]int{p1, p2, p3}
	for pi := 0; pi < len(pa); pi++ {
		p := pa[pi]
		for i := 0; i < len(p); i++ {
			for j := 0; j < len(p); j++ {
				ij := partitionedPort(tag, p[i], p[j])
				pj := port(tag, p[j])
				err := os.Link(pj, ij)
				if err != nil {
					// one reason this link can fail is if the
					// corresponding Paxos peer has prematurely quit and
					// deleted its socket file (e.g., called px.Kill()).
					return fmt.Errorf("os.Link(%v, %v): %v", pj, ij, err)
				}
			}
		}
	}
	return nil
}
