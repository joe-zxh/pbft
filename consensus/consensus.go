package consensus

import (
	"bytes"
	"context"
	"fmt"
	"go.uber.org/atomic"
	"log"
	"sync"
	"time"

	"github.com/joe-zxh/pbft/config"
	"github.com/joe-zxh/pbft/data"
	"github.com/joe-zxh/pbft/internal/logging"
	"github.com/joe-zxh/pbft/util"
)

const (
	changeViewTimeout = 60 * time.Second
	checkpointDiv     = 2000000
)

var logger *log.Logger

func init() {
	logger = logging.GetLogger()
}

// PBFTCore is the safety core of the PBFTCore protocol
type PBFTCore struct {
	// from hotstuff

	cmdCache *data.CommandSet // Contains the commands that are waiting to be proposed
	Config   *config.ReplicaConfig
	cancel   context.CancelFunc // stops goroutines

	Exec chan []data.Command

	// from pbft
	Mut           sync.Mutex // Lock for all internal data
	ID            uint32
	TSeq          atomic.Uint32 // Total sequence number of next request
	View          uint32
	BroadcastView uint32                       // 用于辅助view change实验的...避免多次发起同一个view的view-change...
	Apply         uint32                       // Sequence number of last executed request
	Log           map[data.EntryID]*data.Entry // bycon的log是一个数组，因为需要保证连续，leader可以处理log inconsistency，而pbft不需要。client只有执行完上一条指令后，才会发送下一条请求，所以顺序 并没有问题。
	CPs           map[uint32]*data.CheckPoint
	WaterLow      uint32
	WaterHigh     uint32
	F             uint32
	q             uint32
	N             uint32
	monitor       bool
	Change        *time.Timer
	Changing      bool                // Indicate if this node is changing view
	state         interface{}         // Deterministic state machine's state
	ApplyQueue    *util.PriorityQueue // 因为PBFT的特殊性(log是一个map，而不是list)，所以这里需要一个applyQueue。
	VCs           map[uint32][]*data.ViewChangeArgs
	lastcp        uint32

	Leader   uint32 // view改变的时候，再改变
	IsLeader bool   // view改变的时候，再改变

	ViewChangeChan chan struct{}
}

func (pbft *PBFTCore) AddCommand(command data.Command) {
	pbft.cmdCache.Add(command)
}

func (pbft *PBFTCore) CommandSetLen(command data.Command) int {
	return pbft.cmdCache.Len()
}

// CreateProposal creates a new proposal
func (pbft *PBFTCore) CreateProposal(timeout bool) *data.PrePrepareArgs {

	var batch []data.Command

	if timeout { // timeout的时候，不管够不够batch都要发起共识。
		batch = pbft.cmdCache.RetriveFirst(pbft.Config.BatchSize)
	} else {
		batch = pbft.cmdCache.RetriveExactlyFirst(pbft.Config.BatchSize)
	}

	if batch == nil {
		return nil
	}
	e := &data.PrePrepareArgs{
		View:     pbft.View,
		Seq:      pbft.TSeq.Inc(),
		Commands: batch,
	}
	return e
}

// New creates a new PBFTCore instance
func New(conf *config.ReplicaConfig) *PBFTCore {
	logger.SetPrefix(fmt.Sprintf("hs(id %d): ", conf.ID))

	ctx, cancel := context.WithCancel(context.Background())

	pbft := &PBFTCore{
		// from hotstuff
		Config:   conf,
		cancel:   cancel,
		cmdCache: data.NewCommandSet(),
		Exec:     make(chan []data.Command, 1),

		// pbft
		ID:            uint32(conf.ID),
		View:          1,
		BroadcastView: 1,
		Apply:         0,
		Log:           make(map[data.EntryID]*data.Entry),
		CPs:           make(map[uint32]*data.CheckPoint),
		WaterLow:      0,
		WaterHigh:     2 * checkpointDiv,
		F:             uint32(len(conf.Replicas)-1) / 3,
		N:             uint32(len(conf.Replicas)),
		monitor:       false,
		Change:        nil,
		Changing:      false,
		state:         make([]interface{}, 1),
		ApplyQueue:    util.NewPriorityQueue(),
		VCs:           make(map[uint32][]*data.ViewChangeArgs),
		lastcp:        0,

		ViewChangeChan: make(chan struct{}, 1),
	}
	pbft.q = pbft.F*2 + 1
	pbft.Leader = (pbft.View-1)%pbft.N + 1
	pbft.IsLeader = (pbft.Leader == pbft.ID)

	// Put an initial stable checkpoint
	cp := pbft.getCheckPoint(0)
	cp.Stable = true
	cp.State = pbft.state

	go pbft.proposeConstantly(ctx)

	return pbft
}

func (pbft *PBFTCore) proposeConstantly(ctx context.Context) {
	for {
		select {
		// todo: 一个计时器，如果是leader，就开始preprepare
		case <-ctx.Done():
			return
		}
	}
}

func (pbft *PBFTCore) Close() {
	pbft.cancel()
}

func (pbft *PBFTCore) GetExec() chan []data.Command {
	return pbft.Exec
}

func (pbft *PBFTCore) GetEntry(id data.EntryID) *data.Entry {
	_, ok := pbft.Log[id]
	if !ok {
		pbft.Log[id] = &data.Entry{}
	}
	return pbft.Log[id]
}

// Lock ent.lock before call this function
// Locks : acquire s.lock before call this function
func (pbft *PBFTCore) Prepared(ent *data.Entry) bool {
	if len(ent.P) > int(2*pbft.F) {
		// Key is the id of sender replica
		validSet := make(map[uint32]bool)
		for i, sz := 0, len(ent.P); i < sz; i++ {
			if ent.P[i].View == ent.PP.View && ent.P[i].Seq == ent.PP.Seq && bytes.Equal(ent.P[i].Digest[:], ent.Digest[:]) {
				validSet[ent.P[i].Sender] = true
			}
		}
		return len(validSet) > int(2*pbft.F)
	}
	return false
}

// Locks : acquire s.lock before call this function
func (pbft *PBFTCore) Committed(ent *data.Entry) bool {
	if len(ent.C) > int(2*pbft.F) {
		// Key is replica id
		validSet := make(map[uint32]bool)
		for i, sz := 0, len(ent.C); i < sz; i++ {
			if ent.C[i].View == ent.PP.View && ent.C[i].Seq == ent.PP.Seq && bytes.Equal(ent.C[i].Digest[:], ent.Digest[:]) {
				validSet[ent.C[i].Sender] = true
			}
		}
		return len(validSet) > int(2*pbft.F)
	}
	return false
}

///////////// view changes...

func (pbft *PBFTCore) CalcMinMaxspp(nvArgs *data.NewViewArgs) (uint32, uint32, map[uint32]*data.PrePrepareArgs) {
	var mins uint32 = 0
	var maxs uint32 = 0
	pprepared := make(map[uint32]*data.PrePrepareArgs)
	for i, sz := 0, len(nvArgs.V); i < sz; i++ {
		if nvArgs.V[i].CP.Seq > mins {
			mins = nvArgs.V[i].CP.Seq
		}
		for j, psz := 0, len(nvArgs.V[i].P); j < psz; j++ {
			if nvArgs.V[i].P[j].PP.Seq > maxs {
				maxs = nvArgs.V[i].P[j].PP.Seq
			}
			pprepared[nvArgs.V[i].P[j].PP.Seq] = nvArgs.V[i].P[j].PP
		}
	}
	return mins, maxs, pprepared
}

func (pbft *PBFTCore) CalcPPS(view uint32, mins uint32, maxs uint32, pprepared map[uint32]*data.PrePrepareArgs) []*data.PrePrepareArgs {
	pps := make([]*data.PrePrepareArgs, 0)
	for i := mins + 1; i <= maxs; i++ {
		v, ok := pprepared[i]
		if ok {
			pps = append(pps, &data.PrePrepareArgs{
				View:     view,
				Seq:      i,
				Commands: v.Commands,
			})
		} else {
			pps = append(pps, &data.PrePrepareArgs{
				View: view,
				Seq:  i,
			})
		}
	}
	return pps
}

// Should lock s.lock before call this function
func (pbft *PBFTCore) RemoveOldViewChange(seq uint32) {
	for k := range pbft.VCs {
		if k < seq {
			delete(pbft.VCs, k)
		}
	}
}
