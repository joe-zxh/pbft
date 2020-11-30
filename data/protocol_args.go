package data

import (
	"sync"
)

type PrePrepareArgs struct {
	View     uint32
	Seq      uint32
	Commands []Command
}

type PrepareArgs struct {
	View   uint32
	Seq    uint32
	Digest EntryHash
	Sender uint32
}

type CommitArgs struct {
	View   uint32
	Seq    uint32
	Digest EntryHash
	Sender uint32
}


// Pm is use to hold a preprepare message and at least 2f corresponding prepare message
type Pm struct {
	PP *PrePrepareArgs
	P  []*PrepareArgs
}

// ViewChangeArgs is the argument for RPC handler PBFTServer.ViewChange
type ViewChangeArgs struct {
	View uint32
	CP   *CheckPoint
	P    []*Pm
	Rid  uint32
}

// CheckPoint is the reply of FetchCheckPoint, signature is only set when it transmit by RPC
type CheckPoint struct {
	// lock is unexported, to avoid gob encode this value
	lock   sync.Mutex
	Seq    uint32
	Stable bool
	State  interface{}
	Proof  []*CheckPointArgs
	// Signature
}

// CheckPointArgs is the argument for RPC handler PBFTServer.CheckPoint
type CheckPointArgs struct {
	Seq    uint32
	Digest string
	Rid    uint32
}

type NewViewArgs struct {
	View uint32
	V    []*ViewChangeArgs
	O    []*PrePrepareArgs
}