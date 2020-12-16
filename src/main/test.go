package main

import (
	"fmt"
	"math/rand"
)

const (
	MinElectionTimeOut int = 5000 // in Millisecond
	MaxElectionTimeOut int = 8000 // in Millisecond
	ElectionTimerCheckInterval int = 10 // Interval for the check go routine to check if it should start an election
	WaitVoteTimeOut = 50
	HeartBeatInterval = 10
	Follower int = 0
	Candidate int = 1
	Leader int = 2
)
func main() {

	//testTime()
	testRandom()
}

func testRandom() {

	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
	fmt.Println(randomIntInRange(MinElectionTimeOut, MaxElectionTimeOut))
}
func randomIntInRange(min int, max int) int {

	return rand.Intn(max - min) + min
}
func testTime() {

	//a := time.Now()
	//fmt.Println(a)
	//time.Sleep(time.Second * 5)
	//b := time.Now()
	//fmt.Println(b)
	//fmt.Println(a.Sub(b))
	fmt.Println(5 / 2)
}
