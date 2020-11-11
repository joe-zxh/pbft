package consensus

import (
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

	exec chan []data.Command

	// from pbft
	Lock       sync.Mutex // Lock for all internal data
	id         uint32
	tSeq       atomic.Uint32           // Total sequence number of next request
	seqmap     map[data.EntryID]uint32 // Use to map {Cid,CSeq} to global sequence number for all prepared message
	View       uint32
	apply      uint32                       // Sequence number of last executed request
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
	applyQueue *util.PriorityQueue // 因为PBFT的特殊性(log是一个map，而不是list)，所以这里需要一个applyQueue。
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

func (pbft *PBFTCore) commit(entry *data.Entry) {
	// only called from within update. Thus covered by its mutex lock.
	if pbft.bExec.Height < block.Height {
		if parent, ok := pbft.Blocks.ParentOf(block); ok {
			hs.commit(parent) // todo: 递归改循环
		}
		entry.Committed = true
		logger.Println("EXEC", entry)
		pbft.exec <- entry.Commands
	}
}

// CreateProposal creates a new proposal
func (pbft *PBFTCore) CreateProposal(timeout bool) *data.Entry {

	var batch []data.Command

	if timeout { // timeout的时候，不管够不够batch都要发起共识。
		batch = pbft.cmdCache.GetFirst(pbft.Config.BatchSize)
	} else {
		batch = pbft.cmdCache.GetExactlyFirst(pbft.Config.BatchSize)
	}

	if batch == nil {
		return nil
	}
	e := &data.Entry{
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
		exec:     make(chan []data.Command, 1),

		// pbft
		id:         uint32(conf.ID),
		seqmap:     make(map[data.EntryID]uint32),
		View:       1,
		apply:      -1,
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
		applyQueue: util.NewPriorityQueue(),
		vcs:        make(map[uint32][]*ViewChangeArgs),
		lastcp:     -1,
	}
	pbft.q = pbft.f*2 + 1
	pbft.Leader = (pbft.View-1)%pbft.n + 1
	pbft.IsLeader = (pbft.Leader == pbft.id)

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
	return pbft.exec
}

func (pbft *PBFTCore) GetEntry(id data.EntryID) *data.Entry {
	_, ok := pbft.Log[id]
	if !ok {
		pbft.Log[id] = &data.Entry{
			P: nil,
			C: nil,
		}
	}
	return pbft.Log[id]
}