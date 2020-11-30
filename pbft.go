package pbft

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"github.com/joe-zxh/pbft/util"
	"log"
	"math/big"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/joe-zxh/pbft/config"
	"github.com/joe-zxh/pbft/consensus"
	"github.com/joe-zxh/pbft/data"
	"github.com/joe-zxh/pbft/internal/logging"
	"github.com/joe-zxh/pbft/internal/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

var logger *log.Logger

func init() {
	logger = logging.GetLogger()
}

// PBFT is a thing
type PBFT struct {
	*consensus.PBFTCore
	tls bool

	nodes map[config.ReplicaID]*proto.Node

	server  *pbftServer
	manager *proto.Manager
	cfg     *proto.Configuration

	closeOnce sync.Once

	connectTimeout time.Duration
}

//New creates a new backend object.
func New(conf *config.ReplicaConfig, tls bool, connectTimeout, qcTimeout time.Duration) *PBFT {
	pbft := &PBFT{
		PBFTCore:       consensus.New(conf),
		nodes:          make(map[config.ReplicaID]*proto.Node),
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
	idMapping := make(map[string]uint32, len(pbft.Config.Replicas)-1)
	for _, replica := range pbft.Config.Replicas {
		if replica.ID != pbft.Config.ID {
			idMapping[replica.Address] = uint32(replica.ID)
		}
	}

	// embed own ID to allow other replicas to identify messages from this replica
	md := metadata.New(map[string]string{
		"id": fmt.Sprintf("%d", pbft.Config.ID),
	})

	perNodeMD := func(nid uint32) metadata.MD {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], nid)
		hash := sha512.Sum512(b[:])
		R, S, err := ecdsa.Sign(rand.Reader, pbft.Config.PrivateKey, hash[:])
		if err != nil {
			panic(fmt.Errorf("Could not sign proof for replica %d: %w", nid, err))
		}
		md := metadata.MD{}
		md.Append("proof", base64.StdEncoding.EncodeToString(R.Bytes()), base64.StdEncoding.EncodeToString(S.Bytes()))
		return md
	}

	mgrOpts := []proto.ManagerOption{
		proto.WithDialTimeout(connectTimeout),
		proto.WithNodeMap(idMapping),
		proto.WithMetadata(md),
		proto.WithPerNodeMetadata(perNodeMD),
	}
	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
	}

	if pbft.tls {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(pbft.Config.CertPool, "")))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	mgrOpts = append(mgrOpts, proto.WithGrpcDialOptions(grpcOpts...))

	mgr, err := proto.NewManager(mgrOpts...)
	if err != nil {
		return fmt.Errorf("Failed to connect to replicas: %w", err)
	}
	pbft.manager = mgr

	for _, node := range mgr.Nodes() {
		pbft.nodes[config.ReplicaID(node.ID())] = node
	}

	pbft.cfg, err = pbft.manager.NewConfiguration(pbft.manager.NodeIDs(), &struct{}{})
	if err != nil {
		return fmt.Errorf("Failed to create configuration: %w", err)
	}

	return nil
}

// startServer runs a new instance of pbftServer
func (pbft *PBFT) startServer(port string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		return fmt.Errorf("Failed to listen to port %s: %w", port, err)
	}

	serverOpts := []proto.ServerOption{}
	grpcServerOpts := []grpc.ServerOption{}

	if pbft.tls {
		grpcServerOpts = append(grpcServerOpts, grpc.Creds(credentials.NewServerTLSFromCert(pbft.Config.Cert)))
	}

	serverOpts = append(serverOpts, proto.WithGRPCServerOptions(grpcServerOpts...))

	pbft.server = newPBFTServer(pbft, proto.NewGorumsServer(serverOpts...))
	pbft.server.RegisterPBFTServer(pbft.server)

	go pbft.server.Serve(lis)
	return nil
}

// Close closes all connections made by the PBFT instance
func (pbft *PBFT) Close() {
	pbft.closeOnce.Do(func() {
		pbft.PBFTCore.Close()
		pbft.manager.Close()
		pbft.server.Stop()
	})
}

// Propose broadcasts a new proposal(Pre-Prepare) to all replicas
func (pbft *PBFT) Propose(timeout bool) {
	pp := pbft.CreateProposal(timeout)
	if pp == nil {
		return
	}
	logger.Printf("[B/PrePrepare]: view: %d, seq: %d, (%d commands)\n", pp.View, pp.Seq, len(pp.Commands))
	protobuf := proto.PP2Proto(pp)
	pbft.cfg.PrePrepare(protobuf) // 通过gorums的cfg进行multicast，multicast应该是 不会发送消息给自己的。
	pbft.handlePrePrepare(pp)     // leader自己也要处理proposal
}

// 这个server是面向 集群内部的。
type pbftServer struct {
	*PBFT
	*proto.GorumsServer
	// maps a stream context to client info
	mut     sync.RWMutex
	clients map[context.Context]config.ReplicaID
}

func newPBFTServer(pbft *PBFT, srv *proto.GorumsServer) *pbftServer {
	pbftSrv := &pbftServer{
		PBFT:         pbft,
		GorumsServer: srv,
		clients:      make(map[context.Context]config.ReplicaID),
	}
	return pbftSrv
}

func (pbft *pbftServer) getClientID(ctx context.Context) (config.ReplicaID, error) {
	pbft.mut.RLock()
	// fast path for known stream
	if id, ok := pbft.clients[ctx]; ok {
		pbft.mut.RUnlock()
		return id, nil
	}

	pbft.mut.RUnlock()
	pbft.mut.Lock()
	defer pbft.mut.Unlock()

	// cleanup finished streams
	for ctx := range pbft.clients {
		if ctx.Err() != nil {
			delete(pbft.clients, ctx)
		}
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, fmt.Errorf("getClientID: metadata not available")
	}

	v := md.Get("id")
	if len(v) < 1 {
		return 0, fmt.Errorf("getClientID: id field not present")
	}

	id, err := strconv.Atoi(v[0])
	if err != nil {
		return 0, fmt.Errorf("getClientID: cannot parse ID field: %w", err)
	}

	info, ok := pbft.Config.Replicas[config.ReplicaID(id)]
	if !ok {
		return 0, fmt.Errorf("getClientID: could not find info about id '%d'", id)
	}

	v = md.Get("proof")
	if len(v) < 2 {
		return 0, fmt.Errorf("getClientID: No proof found")
	}

	var R, S big.Int
	v0, err := base64.StdEncoding.DecodeString(v[0])
	if err != nil {
		return 0, fmt.Errorf("getClientID: could not decode proof: %v", err)
	}
	v1, err := base64.StdEncoding.DecodeString(v[1])
	if err != nil {
		return 0, fmt.Errorf("getClientID: could not decode proof: %v", err)
	}
	R.SetBytes(v0)
	S.SetBytes(v1)

	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(pbft.Config.ID))
	hash := sha512.Sum512(b[:])

	if !ecdsa.Verify(info.PubKey, hash[:], &R, &S) {
		return 0, fmt.Errorf("Invalid proof")
	}

	pbft.clients[ctx] = config.ReplicaID(id)
	return config.ReplicaID(id), nil
}

func (pbft *PBFT) handlePrePrepare(pp *data.PrePrepareArgs) {

	pbft.PBFTCore.Mut.Lock()

	if !pbft.Changing && pbft.View == pp.View {

		ent := pbft.GetEntry(data.EntryID{V: pp.View, N: pp.Seq})
		pbft.PBFTCore.Mut.Unlock()

		ent.Mut.Lock()
		if ent.Digest == nil {
			ent.PP = pp
			ent.Hash()
			p := &proto.PrepareArgs{
				View:   pp.View,
				Seq:    pp.Seq,
				Digest: ent.Digest.ToSlice(),
			}
			ent.Mut.Unlock()

			logger.Printf("[B/Prepare]: view: %d, seq: %d\n", pp.View, pp.Seq)
			pbft.cfg.Prepare(p)
			dp := p.Proto2P()
			dp.Sender = pbft.ID
			pbft.handlePrepare(dp)
		} else {
			ent.Mut.Unlock()
			fmt.Println(`接收到多个具有相同seq的preprepare`)
		}

	} else {
		pbft.PBFTCore.Mut.Unlock()
	}
}

func (pbft *pbftServer) PrePrepare(ctx context.Context, protoPP *proto.PrePrepareArgs) {
	dpp := protoPP.Proto2PP()
	id, err := pbft.getClientID(ctx)
	if err != nil {
		logger.Printf("Failed to get client ID: %v", err)
		return
	}
	if uint32(id) == pbft.Leader { // 只处理来自leader的preprepare
		pbft.handlePrePrepare(dpp)
	}
}

func (pbft *PBFT) handlePrepare(p *data.PrepareArgs) {

	pbft.Mut.Lock()

	if !pbft.Changing && pbft.View == p.View {
		ent := pbft.GetEntry(data.EntryID{p.View, p.Seq})
		pbft.Mut.Unlock()

		ent.Mut.Lock()

		ent.P = append(ent.P, p)
		if ent.PP != nil && !ent.SendCommit && pbft.Prepared(ent) {

			c := &proto.CommitArgs{
				View:   ent.PP.View,
				Seq:    ent.PP.Seq,
				Digest: ent.Digest.ToSlice(),
			}

			ent.SendCommit = true
			ent.Mut.Unlock()

			logger.Printf("[B/Commit]: view: %d, seq: %d\n", p.View, p.Seq)
			pbft.cfg.Commit(c)
			dc := c.Proto2C()
			dc.Sender = pbft.ID
			pbft.handleCommit(dc)
		} else {
			ent.Mut.Unlock()
		}
	} else {
		pbft.Mut.Unlock()
	}
}

func (pbft *pbftServer) Prepare(ctx context.Context, protoP *proto.PrepareArgs) {
	dp := protoP.Proto2P()
	id, err := pbft.getClientID(ctx)
	if err != nil {
		logger.Printf("Failed to get client ID: %v", err)
		return
	}
	dp.Sender = uint32(id)
	pbft.handlePrepare(dp)
}

func (pbft *PBFT) handleCommit(c *data.CommitArgs) {
	pbft.Mut.Lock()

	if !pbft.Changing && pbft.View == c.View {
		ent := pbft.GetEntry(data.EntryID{c.View, c.Seq})
		pbft.Mut.Unlock()

		ent.Mut.Lock()
		ent.C = append(ent.C, c)
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
}

func (pbft *pbftServer) Commit(ctx context.Context, protoC *proto.CommitArgs) {
	dc := protoC.Proto2C()
	id, err := pbft.getClientID(ctx)
	if err != nil {
		logger.Printf("Failed to get client ID: %v", err)
		return
	}
	dc.Sender = uint32(id)
	pbft.handleCommit(dc)
}

// view change...
func (pbft *PBFT) StartViewChange() {
	pbft.Mut.Lock()
	logger.Printf("StartViewChange: \n")
	vcArgs := pbft.GenerateViewChange()
	pbft.Mut.Unlock()

	logger.Printf("Broadcast ViewChange:Args:%v\n", *vcArgs)
	pbft.cfg.ViewChange(proto.VC2Proto(vcArgs)) // 自己不需要处理，到2f个的时候，新leader再生成vcArgs
}

func (pbft *pbftServer) ViewChange(ctx context.Context, protoVC *proto.ViewChangeArgs) {
	pbft.Mut.Lock()
	defer pbft.Mut.Unlock()

	args := protoVC.Proto2VC()

	logger.Printf("Receive ViewChange: args: %v\n", args)

	// Ignore old viewchange message
	if args.View <= pbft.View || args.Rid == pbft.ID {
		return
	}

	_, ok := pbft.VCs[args.View]
	if !ok {
		pbft.VCs[args.View] = make([]*data.ViewChangeArgs, 0)
	}

	// Insert this view change message to its log
	pbft.VCs[args.View] = append(pbft.VCs[args.View], args)

	// Leader entering new view
	if args.View%pbft.N == pbft.ID && len(pbft.VCs[args.View]) >= 2*int(pbft.F) {
		nvArgs := data.NewViewArgs{
			View: args.View,
			V:    pbft.VCs[args.View],
		}
		nvArgs.V = append(nvArgs.V, pbft.GenerateViewChange())

		mins, maxs, pprepared := pbft.CalcMinMaxspp(&nvArgs)
		pps := pbft.CalcPPS(args.View, mins, maxs, pprepared)

		nvArgs.O = pps

		logger.Printf("Broadcast NewView:Args:%v", nvArgs)

		pbft.cfg.NewView(proto.NV2Proto(&nvArgs))
		pbft.EnteringNewView(&nvArgs, mins, maxs, pps)
	}
}

func (pbft *pbftServer) NewView(ctx context.Context, protoNV *proto.NewViewArgs) {
	args := protoNV.Proto2NV()

	pbft.Mut.Lock()
	defer pbft.Mut.Unlock()

	if args.View <= pbft.View {
		return
	}

	logger.Printf("Receive NewView:Args:%v", args)

	// Verify V sest
	vcs := make(map[uint32]bool)
	for i, sz := 0, len(args.V); i < sz; i++ {
		if args.V[i].View == args.View {
			vcs[args.V[i].Rid] = true
		}
	}
	if len(vcs) <= int(2*pbft.F) {
		panic("[V/NewView/Fail] view change message is not enough")
		return
	}

	// Verify O set
	mins, maxs, pprepared := pbft.CalcMinMaxspp(args)
	pps := pbft.CalcPPS(args.View, mins, maxs, pprepared)

	for i, sz := 0, len(pps); i < sz; i++ {
		if pps[i].View != args.O[i].View || pps[i].Seq != args.O[i].Seq || data.CommandsEqual(pps[i].Commands, args.O[i].Commands) {
			panic("NewView Fail: PrePrepare message missmatch : %+v")
		}
	}

	pbft.EnteringNewView(args, mins, maxs, pps)
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

	pbft.TSeq.Store(maxs + 1)
	ps := make([]*data.PrepareArgs, len(pps))
	for i, sz := 0, len(pps); i < sz; i++ {
		ent := pbft.GetEntry(data.EntryID{nvArgs.View, pps[i].Seq})
		ent.PP = pps[i]

		pArgs := data.PrepareArgs{
			View:   nvArgs.View,
			Seq:    pps[i].Seq,
			Digest: ent.Hash(),
			Sender: pbft.ID,
		}
		ps[i] = &pArgs
	}
	pbft.View = nvArgs.View
	pbft.Changing = false
	pbft.RemoveOldViewChange(nvArgs.View)

	// 回消息给client...
	pbft.ViewChangeChan <- struct{}{}

	go func() {
		pbft.Mut.Lock()
		pbft.Mut.Unlock()
		for i, sz := 0, len(ps); i < sz; i++ {
			logger.Printf("Broadcast Prepare:Args:%v", pbft, ps[i])
			pbft.cfg.Prepare(proto.P2Proto(ps[i]))
			time.Sleep(5 * time.Millisecond)
		}
	}()

	return ps
}
