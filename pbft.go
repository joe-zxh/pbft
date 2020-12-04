package pbft

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/joe-zxh/pbft/util"
	"log"
	"net"
	"sync"
	"time"

	"github.com/joe-zxh/pbft/config"
	"github.com/joe-zxh/pbft/consensus"
	"github.com/joe-zxh/pbft/data"
	"github.com/joe-zxh/pbft/internal/logging"
	"github.com/joe-zxh/pbft/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var logger *log.Logger

func init() {
	logger = logging.GetLogger()
}

// PBFT is a thing
type PBFT struct {
	*consensus.PBFTCore
	proto.UnimplementedPBFTServer

	tls bool

	nodes map[config.ReplicaID]*proto.PBFTClient
	conns map[config.ReplicaID]*grpc.ClientConn

	server *pbftServer

	closeOnce      sync.Once
	connectTimeout time.Duration
}

//New creates a new backend object.
func New(conf *config.ReplicaConfig, tls bool, connectTimeout, qcTimeout time.Duration) *PBFT {
	pbft := &PBFT{
		PBFTCore:       consensus.New(conf),
		nodes:          make(map[config.ReplicaID]*proto.PBFTClient),
		conns:          make(map[config.ReplicaID]*grpc.ClientConn),
		connectTimeout: connectTimeout,
	}
	return pbft
}

//Start starts the server and client
func (pbft *PBFT) Start() error {
	addr := pbft.Config.Replicas[pbft.Config.ID].Address
	err := pbft.startServer(addr)
	if err != nil {
		return fmt.Errorf("Failed to start GRPC Server: %w", err)
	}
	err = pbft.startClient(pbft.connectTimeout)
	if err != nil {
		return fmt.Errorf("Failed to start GRPC Clients: %w", err)
	}
	return nil
}

// 作为rpc的client端，调用其他hsserver的rpc。
func (pbft *PBFT) startClient(connectTimeout time.Duration) error {
	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
	}

	if pbft.tls {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pbft.Config.CertPool, "")))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	for rid, replica := range pbft.Config.Replicas {
		if replica.ID != pbft.Config.ID {
			conn, err := grpc.Dial(replica.Address, grpcOpts...)
			if err != nil {
				log.Fatalf("connect error: %v", err)
				conn.Close()
			} else {
				pbft.conns[rid] = conn
				c := proto.NewPBFTClient(conn)
				pbft.nodes[rid] = &c
			}
		}
	}

	return nil
}

// startServer runs a new instance of pbftServer
func (pbft *PBFT) startServer(port string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("Failed to listen to port %s: %w", port, err)
	}

	grpcServerOpts := []grpc.ServerOption{}

	if pbft.tls {
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(credentials.NewServerTLSFromCert(pbft.Config.Cert)))
	}

	pbft.server = newPBFTServer(pbft)

	s := grpc.NewServer(grpcServerOpts...)
	proto.RegisterPBFTServer(s, pbft.server)

	go s.Serve(lis)
	return nil
}

// Close closes all connections made by the PBFT instance
func (pbft *PBFT) Close() {
	pbft.closeOnce.Do(func() {
		pbft.PBFTCore.Close()
		for _, conn := range pbft.conns { // close clients connections
			conn.Close()
		}
	})
}

// 这个server是面向 集群内部的。
type pbftServer struct {
	*PBFT

	mut     sync.RWMutex
	clients map[context.Context]config.ReplicaID
}

func newPBFTServer(pbft *PBFT) *pbftServer {
	pbftSrv := &pbftServer{
		PBFT:    pbft,
		clients: make(map[context.Context]config.ReplicaID),
	}
	return pbftSrv
}

// Propose broadcasts a new proposal(Pre-Prepare) to all replicas
func (pbft *PBFT) Propose(timeout bool) {
	dPP := pbft.CreateProposal(timeout)
	if dPP == nil {
		return
	}

	go func() {
		pPP := proto.PP2Proto(dPP)
		pPP.Sender = pbft.ID
		go pbft.BroadcastPrePrepareRequest(pPP)
		pbft.server.PrePrepare(context.TODO(), pPP) // leader自己也要处理proposal
	}()
}

func (pbft *PBFT) BroadcastPrePrepareRequest(pPP *proto.PrePrepareArgs) {
	logger.Printf("[B/PrePrepare]: view: %d, seq: %d, (%d commands)\n", pPP.View, pPP.Seq, len(pPP.Commands))

	for rid, client := range pbft.nodes {
		if rid != pbft.Config.ID {
			go func(id config.ReplicaID, cli *proto.PBFTClient) {
				_, err := (*cli).PrePrepare(context.TODO(), pPP)
				if err != nil {
					panic(err)
				}
			}(rid, client)
		}
	}
}

func (pbft *PBFT) BroadcastPrepareRequest(pP *proto.PrepareArgs) {
	logger.Printf("[B/Prepare]: view: %d, seq: %d\n", pP.View, pP.Seq)

	for rid, client := range pbft.nodes {

		if rid != pbft.Config.ID {
			go func(id config.ReplicaID, cli *proto.PBFTClient) {
				_, err := (*cli).Prepare(context.TODO(), pP)
				if err != nil {
					panic(err)
				}
			}(rid, client)
		}
	}
}

func (pbft *PBFT) BroadcastCommitRequest(pC *proto.CommitArgs) {
	logger.Printf("[B/Commit]: view: %d, seq: %d\n", pC.View, pC.Seq)

	for rid, client := range pbft.nodes {
		if rid != pbft.Config.ID {
			go func(id config.ReplicaID, cli *proto.PBFTClient) {
				_, err := (*cli).Commit(context.TODO(), pC)
				if err != nil {
					panic(err)
				}
			}(rid, client)
		}
	}
}

func (pbft *pbftServer) PrePrepare(_ context.Context, pPP *proto.PrePrepareArgs) (*empty.Empty, error) {

	logger.Printf("PrePrepare: view:%d, seq:%d\n", pPP.View, pPP.Seq)

	dPP := pPP.Proto2PP()

	pbft.PBFTCore.Mut.Lock()
	if !pbft.Changing && pbft.View == dPP.View {
		ent := pbft.GetEntry(data.EntryID{V: dPP.View, N: dPP.Seq})
		pbft.PBFTCore.Mut.Unlock()

		ent.Mut.Lock()
		if ent.Digest == nil {
			ent.PP = dPP
			ent.Hash()
			pP := &proto.PrepareArgs{
				Sender: pbft.ID,
				View:   dPP.View,
				Seq:    dPP.Seq,
				Digest: ent.Digest.ToSlice(),
			}
			ent.Mut.Unlock()

			logger.Printf("[B/Prepare]: view: %d, seq: %d\n", dPP.View, dPP.Seq)

			go pbft.BroadcastPrepareRequest(pP)
			go pbft.Prepare(context.TODO(), pP)
		} else {
			ent.Mut.Unlock()
			return &empty.Empty{}, errors.New(`接收到多个具有相同seq的preprepare`)
		}

	} else {
		pbft.PBFTCore.Mut.Unlock()
		return &empty.Empty{}, errors.New(`正在view change 或者 view不匹配`)
	}

	return &empty.Empty{}, nil
}

func (pbft *pbftServer) Prepare(ctx context.Context, pP *proto.PrepareArgs) (*empty.Empty, error) {
	dP := pP.Proto2P()
	pbft.Mut.Lock()

	if !pbft.Changing && pbft.View == dP.View {
		ent := pbft.GetEntry(data.EntryID{dP.View, dP.Seq})
		pbft.Mut.Unlock()

		ent.Mut.Lock()

		ent.P = append(ent.P, dP)
		if ent.PP != nil && !ent.SendCommit && pbft.Prepared(ent) {

			pC := &proto.CommitArgs{
				Sender: pbft.ID,
				View:   ent.PP.View,
				Seq:    ent.PP.Seq,
				Digest: ent.Digest.ToSlice(),
			}

			ent.SendCommit = true
			ent.Mut.Unlock()

			logger.Printf("[B/Commit]: view: %d, seq: %d\n", dP.View, dP.Seq)

			go pbft.BroadcastCommitRequest(pC)
			go pbft.Commit(context.TODO(), pC)
		} else {
			ent.Mut.Unlock()
		}
	} else {
		pbft.Mut.Unlock()
	}

	return &empty.Empty{}, nil
}

func (pbft *pbftServer) Commit(ctx context.Context, pC *proto.CommitArgs) (*empty.Empty, error) {
	dC := pC.Proto2C()

	pbft.Mut.Lock()

	if !pbft.Changing && pbft.View == dC.View {
		ent := pbft.GetEntry(data.EntryID{dC.View, dC.Seq})
		pbft.Mut.Unlock()

		ent.Mut.Lock()
		ent.C = append(ent.C, dC)
		if !ent.Committed && ent.SendCommit && pbft.Committed(ent) {
			logger.Printf("Committed entry: view: %d, seq: %d\n", ent.PP.View, ent.PP.Seq)
			ent.Committed = true

			elem := util.PQElem{
				Pri: int(ent.PP.Seq), // ent.SendCommit保证了ent.PP不为空
				C:   ent.PP.Commands,
			}
			ent.Mut.Unlock()
			pbft.Mut.Lock()

			inserted := pbft.ApplyQueue.Insert(elem)
			if !inserted {
				panic("Already insert some request with same sequence")
			}

			for i, sz := 0, pbft.ApplyQueue.Length(); i < sz; i++ { // commit需要按global seq的顺序
				m, err := pbft.ApplyQueue.GetMin()
				if err != nil {
					break
				}
				if int(pbft.Apply+1) == m.Pri {
					pbft.Apply++
					cmds, ok := m.C.([]data.Command)
					if ok {
						pbft.Exec <- cmds
					}
					pbft.ApplyQueue.ExtractMin()

				} else if int(pbft.Apply+1) > m.Pri {
					panic("This should already done")
				} else {
					break
				}
			}
			pbft.Mut.Unlock()
		} else {
			ent.Mut.Unlock()
		}
	} else {
		pbft.Mut.Unlock()
	}

	return &empty.Empty{}, nil
}

// view change...
func (pbft *PBFT) StartViewChange() {
	pbft.Mut.Lock()
	logger.Printf("StartViewChange: \n")
	vcArgs := pbft.GenerateViewChange()
	if vcArgs.View <= pbft.BroadcastView { // 避免重复broadcast同一个view的view change，否则会出现[V/NewView/Fail] view change message is not enough
		pbft.Mut.Unlock()
		return
	} else {
		pbft.BroadcastView = vcArgs.View
		pbft.Mut.Unlock()
	}
	logger.Printf("Broadcast ViewChange:Args:%+v\n", *vcArgs)
	pbft.BroadcastViewChange(proto.VC2Proto(vcArgs)) // 自己不需要处理，到2f个的时候，新leader再生成vcArgs
}

func (pbft *PBFT) BroadcastViewChange(pVC *proto.ViewChangeArgs) {
	logger.Printf("[B/ViewChange]: view: %d\n", pVC.View)

	for rid, client := range pbft.nodes {
		if rid != pbft.Config.ID {
			go func(id config.ReplicaID, cli *proto.PBFTClient) {
				_, err := (*cli).ViewChange(context.TODO(), pVC)
				if err != nil {
					panic(err)
				}
			}(rid, client)
		}
	}
}

func (pbft *PBFT) BroadcastNewView(pNV *proto.NewViewArgs) {
	logger.Printf("[B/NewView]: view: %d\n", pNV.View)

	for rid, client := range pbft.nodes {
		if rid != pbft.Config.ID {
			go func(id config.ReplicaID, cli *proto.PBFTClient) {
				_, err := (*cli).NewView(context.TODO(), pNV)
				if err != nil {
					panic(err)
				}
			}(rid, client)
		}
	}
}

func (pbft *pbftServer) ViewChange(ctx context.Context, protoVC *proto.ViewChangeArgs) (*empty.Empty, error) {
	pbft.Mut.Lock()
	defer pbft.Mut.Unlock()

	args := protoVC.Proto2VC()

	logger.Printf("Receive ViewChange: args: %+v\n", args)

	// Ignore old viewchange message
	if args.View <= pbft.View || args.Rid == pbft.ID {
		return &empty.Empty{}, nil
	}

	_, ok := pbft.VCs[args.View]
	if !ok {
		pbft.VCs[args.View] = make([]*data.ViewChangeArgs, 0)
	}

	// Insert this view change message to its log
	pbft.VCs[args.View] = append(pbft.VCs[args.View], args)

	// Leader entering new view
	// if (pbft.View-1)%pbft.N+1 == pbft.ID && len(pbft.VCs[args.View]) >= 2*int(pbft.F) {
	if 1 == pbft.ID && len(pbft.VCs[args.View]) >= 2*int(pbft.F) {
		pbft.VCs[args.View] = append(pbft.VCs[args.View], pbft.GenerateViewChange())
		nvArgs := data.NewViewArgs{
			View: args.View,
			V:    pbft.VCs[args.View],
		}

		mins, maxs, pprepared := pbft.CalcMinMaxspp(&nvArgs)
		pps := pbft.CalcPPS(args.View, mins, maxs, pprepared)

		nvArgs.O = pps

		logger.Printf("Broadcast NewView:Args:%v", nvArgs)

		go pbft.BroadcastNewView(proto.NV2Proto(&nvArgs))
		pbft.EnteringNewView(&nvArgs, mins, maxs, pps)
	}
	return &empty.Empty{}, nil
}

func (pbft *pbftServer) NewView(ctx context.Context, protoNV *proto.NewViewArgs) (*empty.Empty, error) {

	args := protoNV.Proto2NV()

	pbft.Mut.Lock()
	defer pbft.Mut.Unlock()

	if args.View <= pbft.View {
		return &empty.Empty{}, nil
	}

	logger.Printf("Receive NewView:Args:%+v", args)

	// Verify V sest
	vcs := make(map[uint32]bool)
	for i, sz := 0, len(args.V); i < sz; i++ {
		if args.V[i].View == args.View {
			vcs[args.V[i].Rid] = true
		}
	}
	if len(vcs) <= int(2*pbft.F) {
		for i, sz := 0, len(args.V); i < sz; i++ {
			log.Printf("args.V: %+v\n", args.V[i])
		}
		panic("[V/NewView/Fail] view change message is not enough")
	}

	// Verify O set
	mins, maxs, pprepared := pbft.CalcMinMaxspp(args)
	pps := pbft.CalcPPS(args.View, mins, maxs, pprepared)

	for i, sz := 0, len(pps); i < sz; i++ {
		if pps[i].View != args.O[i].View || pps[i].Seq != args.O[i].Seq || !data.CommandsEqual(pps[i].Commands, args.O[i].Commands) {

			log.Printf("pps[i].View != args.O[i].View: %v, pps[i].Seq != args.O[i].Seq: %v, pps[i].Commands: %v, args.O[i].Commands: %v\n",
				pps[i].View != args.O[i].View,
				pps[i].Seq != args.O[i].Seq,
				pps[i].Commands, args.O[i].Commands)

			panic("NewView Fail: PrePrepare message missmatch : %+v")
		}
	}

	pbft.EnteringNewView(args, mins, maxs, pps)
	return &empty.Empty{}, nil
}

func (pbft *PBFT) EnteringNewView(nvArgs *data.NewViewArgs, mins uint32, maxs uint32, pps []*data.PrePrepareArgs) []*data.PrepareArgs {
	logger.Printf("EnterNextView:%v\n", nvArgs.View)

	scp := pbft.GetStableCheckPoint()
	if mins > scp.Seq {
		for i, sz := 0, len(nvArgs.V); i < sz; i++ {
			if nvArgs.V[i].CP.Seq == mins {
				pbft.CPs[mins] = nvArgs.V[i].CP
				break
			}
		}
		// todo: pbft.stablizeCP(pbft.CPs[mins])
	}

	pbft.TSeq.Store(maxs)
	ps := make([]*data.PrepareArgs, len(pps))
	// 注释部分会使s.log会越来越长
	//for i, sz := 0, len(pps); i < sz; i++ {
	//	ent := pbft.GetEntry(data.EntryID{nvArgs.View, pps[i].Seq})
	//	ent.PP = pps[i]
	//
	//	pArgs := data.PrepareArgs{
	//		View:   nvArgs.View,
	//		Seq:    pps[i].Seq,
	//		Digest: ent.Hash(),
	//		Sender: pbft.ID,
	//	}
	//	ps[i] = &pArgs
	//}
	pbft.View = nvArgs.View
	pbft.Changing = false
	pbft.RemoveOldViewChange(nvArgs.View)

	// pbft.Leader = (pbft.View-1)%pbft.N + 1
	pbft.Leader = 1 // 方便实验
	pbft.IsLeader = (pbft.Leader == pbft.ID)

	// 回消息给client...
	pbft.ViewChangeChan <- struct{}{}

	//go func() {
	//	for i, sz := 0, len(ps); i < sz; i++ {
	//		logger.Printf("Broadcast Prepare:Args:%v", pbft, ps[i])
	//		go pbft.cfg.Prepare(proto.P2Proto(ps[i]))
	//		time.Sleep(5 * time.Millisecond)
	//	}
	//}()

	return ps
}
