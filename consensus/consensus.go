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
	cancel   context.CancelFunc // stops any goroutines started by HotStuff

	Exec chan []data.Command

	// from pbft
	Mut        sync.Mutex // Lock for all internal data
	ID         uint32
	tSeq       atomic.Uint32           // Total sequence number of next request
	seqmap     map[data.EntryID]uint32 // Use to map {Cid,CSeq} to global sequence number for all prepared message
	View       uint32
	Apply      uint32                       // Sequence number of last executed request
	Log        map[data.EntryID]*data.Entry // bycon的log是一个数组，因为需要保证连续，leader可以处理log inconsistency，而pbft不需要。client只有执行完上一条指令后，才会发送下一条请求，所以顺序 并没有问题。
	cps        map[int]*CheckPoint
	WaterLow   uint32
	WaterHigh  uint32
	f          uint32
	q          uint32
	n          uint32
	monitor    bool
	Change     *time.Timer
	Changing   bool                // Indicate if this node is changing view
	state      interface{}         // Deterministic state machine's state
	ApplyQueue *util.PriorityQueue // 因为PBFT的特殊性(log是一个map，而不是list)，所以这里需要一个applyQueue。
	vcs        map[uint32][]*ViewChangeArgs
	lastcp     uint32

	Leader   uint32 // view改变的时候，再改变
	IsLeader bool   // view改变的时候，再改变
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
		Seq:      pbft.tSeq.Inc(),
		Commands: batch,
	}
	return e
}

// New creates a new PBFTCore instance
func New(conf *config.ReplicaConfig) *PBFTCore {
	logger.SetPrefix(fmt.Sprintf("hs(id %d): ", conf.ID))

	ctx, cancel := context.WithCancel(context.Background())

	pbft := &PBFTCore{
		// hotstuff
		Config:   conf,
		cancel:   cancel,
		cmdCache: data.NewCommandSet(),
		Exec:     make(chan []data.Command, 1),

		// pbft
		ID:         uint32(conf.ID),
		seqmap:     make(map[data.EntryID]uint32),
		View:       1,
		Apply:      0,
		Log:        make(map[data.EntryID]*data.Entry),
		cps:        make(map[int]*CheckPoint),
		WaterLow:   0,
		WaterHigh:  2 * checkpointDiv,
		f:          uint32(len(conf.Replicas)-1) / 3,
		n:          uint32(len(conf.Replicas)),
		monitor:    false,
		Change:     nil,
		Changing:   false,
		state:      make([]interface{}, 1),
		ApplyQueue: util.NewPriorityQueue(),
		vcs:        make(map[uint32][]*ViewChangeArgs),
		lastcp:     0,
	}
	pbft.q = pbft.f*2 + 1
	pbft.Leader = (pbft.View-1)%pbft.n + 1
	pbft.IsLeader = (pbft.Leader == pbft.ID)

	// Put an initial stable checkpoint
	cp := pbft.getCheckPoint(-1)
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

// Close frees resources held by HotStuff and closes backend connections
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
	if len(ent.P) > int(2*pbft.f) {
		// Key is the id of sender replica
		validSet := make(map[uint32]bool)
		for i, sz := 0, len(ent.P); i < sz; i++ {
			if ent.P[i].View == ent.PP.View && ent.P[i].Seq == ent.PP.Seq && bytes.Equal(ent.P[i].Digest[:], ent.Digest[:]) {
				validSet[ent.P[i].Sender] = true
			}
		}
		return len(validSet) > int(2*pbft.f)
	}
	return false
}

// Locks : acquire s.lock before call this function
func (pbft *PBFTCore) Committed(ent *data.Entry) bool {
	if len(ent.C) > int(2*pbft.f) {
		// Key is replica id
		validSet := make(map[uint32]bool)
		for i, sz := 0, len(ent.C); i < sz; i++ {
			if ent.C[i].View == ent.PP.View && ent.C[i].Seq == ent.PP.Seq && bytes.Equal(ent.C[i].Digest[:], ent.Digest[:]) {
				validSet[ent.C[i].Sender] = true
			}
		}
		return len(validSet) > int(2*pbft.f)
	}
	return false
}
