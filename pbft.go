package pbft

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"fmt"
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

//New creates a new GorumsHotStuff backend object.
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
	pbft.server.RegisterHotstuffServer(pbft.server)

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
	proposal := pbft.CreateProposal(timeout)
	if proposal == nil {
		return
	}
	logger.Printf("Propose (%d commands): %s\n", len(proposal.Commands), proposal)
	protobuf := proto.EntryToPPProto(proposal)
	pbft.cfg.PrePrepare(protobuf)   // 通过gorums的cfg进行multicast，multicast应该是 不会发送消息给自己的。
	pbft.handlePrePrepare(proposal) // leader自己也要处理proposal
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

func (pbft *PBFT) handlePrePrepare(entry *data.Entry) {

	pbft.PBFTCore.Lock.Lock()

	if !pbft.Changing && pbft.View == entry.View && pbft.WaterLow <= entry.Seq && entry.Seq < pbft.WaterHigh {
		pbft.stopTimer()
		pbft.Log[data.EntryID{V: entry.View, N: entry.Seq}] = entry

		pbft.PBFTCore.Lock.Unlock()

		p := &proto.PrepareArgs{
			View: entry.View,
			Seq:  entry.Seq,
			Hash: entry.Hash().ToSlice(),
		}

		pbft.cfg.Prepare(p)
		pbft.handlePrepare(p)
	} else {
		pbft.PBFTCore.Lock.Unlock()
	}
}

func (pbft *pbftServer) PrePrepare(ctx context.Context, protoE *proto.PrePrepareArgs) {
	entry := protoE.PPProto2Entry()
	id, err := pbft.getClientID(ctx)
	if err != nil {
		logger.Printf("Failed to get client ID: %v", err)
		return
	}
	if uint32(id) == pbft.Leader {
		pbft.handlePrePrepare(entry)
	}
}

func (pbft *PBFT) handlePrepare(p *proto.PrepareArgs) {
	s.lock.Lock()
}

func (pbft *pbftServer) Prepare(ctx context.Context, p *proto.PrepareArgs) {
	pbft.handlePrepare(p)
}

func (pbft *pbftServer) Commit(ctx context.Context, msg *proto.CommitArgs) {
	qc := msg.FromProto()
	pbft.OnReceiveNewView(qc)
}

func (pbft *pbftServer) stopTimer() {
	if pbft.Change != nil {
		pbft.Change.Stop()
	}
}
