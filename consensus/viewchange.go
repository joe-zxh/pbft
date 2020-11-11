package consensus

import (
	"github.com/joe-zxh/pbft/internal/proto"
	"sync"
)

// Pm is use to hold a preprepare message and at least 2f corresponding prepare message
type Pm struct {
	PP *proto.PrePrepareArgs
	P  []*proto.PrepareArgs
}

// ViewChangeArgs is the argument for RPC handler PBFTServer.ViewChange
type ViewChangeArgs struct {
	View int
	CP   *CheckPoint
	P    []*Pm
	Rid  int
}

// CheckPoint is the reply of FetchCheckPoint, signature is only set when it transmit by RPC
type CheckPoint struct {
	// lock is unexported, to avoid gob encode this value
	lock   sync.Mutex
	Seq    int
	Stable bool
	State  interface{}
	Proof  []*CheckPointArgs
	// Signature
}

// CheckPointArgs is the argument for RPC handler PBFTServer.CheckPoint
type CheckPointArgs struct {
	Seq    int
	Digest string
	Rid    int
}

// Should lock s.lock before call this function
// When the checkpoint didn't exist then, it must been deleted or it's a new checkpoint
// so this function will also check the hold checkpoint, if there is any checkpoint newer than
// it, it will return nil
func (pbft *PBFTCore) getCheckPoint(seq int) *CheckPoint {
	_, ok := pbft.cps[seq]
	if !ok {
		for k, v := range pbft.cps {
			if k > seq && v.Stable {
				return nil
			}
		}

		pbft.cps[seq] = &CheckPoint{
			Seq:    seq,
			Stable: false,
			State:  nil,
			Proof:  make([]*CheckPointArgs, 0),
		}
	}
	return pbft.cps[seq]
}
