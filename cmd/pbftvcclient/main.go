// client for view change
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joe-zxh/pbft/client"
	"github.com/joe-zxh/pbft/config"
	"github.com/joe-zxh/pbft/data"
	"github.com/relab/gorums/benchmark"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type options struct {
	SelfID      config.ReplicaID `mapstructure:"self-id"`
	RateLimit   int              `mapstructure:"rate-limit"`
	PayloadSize int              `mapstructure:"payload-size"`
	MaxInflight uint64           `mapstructure:"max-inflight"`
	DataSource  string           `mapstructure:"input"`
	Benchmark   bool             `mapstructure:"benchmark"`
	ExitAfter   int              `mapstructure:"exit-after"`
	TLS         bool
	Replicas    []struct {
		ID         config.ReplicaID
		ClientAddr string `mapstructure:"client-address"`
		Pubkey     string
		Cert       string
	}
}

func usage() {
	fmt.Printf("Usage: %s [options]\n", os.Args[0])
	fmt.Println()
	fmt.Println("Loads configuration from ./pbft.toml")
	fmt.Println()
	fmt.Println("Options:")
	pflag.PrintDefaults()
}

func main() {
	pflag.Usage = usage

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	help := pflag.BoolP("help", "h", false, "Prints this text.")
	pflag.Uint32("self-id", 0, "The id for this replica.")
	pflag.Int("payload-size", 30, "The size of the payload in bytes")
	pflag.Bool("tls", true, "Enable TLS")
	clusterSize := pflag.Int("cluster-size", 4, "specify the size of the cluster")
	prepareNum := pflag.Int("prepare-num", 10, "specify the number of prepare entries before view changes...")
	viewchangeNum := pflag.Int("viewchange-num", 10, "specify the number of view changes for experiment...")
	pflag.Parse()

	if *help {
		pflag.Usage()
		os.Exit(0)
	}

	viper.BindPFlags(pflag.CommandLine)

	// read main config file in working dir
	viper.SetConfigName("pbft")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read config: %v\n", err)
		os.Exit(1)
	}

	var conf options
	err = viper.Unmarshal(&conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to unmarshal config: %v\n", err)
		os.Exit(1)
	}

	if *clusterSize > len(conf.Replicas) {
		panic("cluster size too large, you do not have enough replica configuration in the toml file")
	} else {
		conf.Replicas = conf.Replicas[:*clusterSize]
	}

	replicaConfig := config.NewConfig(0, nil, nil)
	for _, r := range conf.Replicas {
		key, err := data.ReadPublicKeyFile(r.Pubkey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to read public key file '%s': %v\n", r.Pubkey, err)
			os.Exit(1)
		}
		if conf.TLS {
			cert, err := data.ReadCertFile(r.Cert)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to read certificate '%s': %v\n", r.Cert, err)
				os.Exit(1)
			}
			replicaConfig.CertPool.AppendCertsFromPEM(cert)
		}
		replicaConfig.Replicas[r.ID] = &config.ReplicaInfo{
			ID:      r.ID,
			Address: r.ClientAddr,
			PubKey:  key,
		}
	}

	client, err := newHotStuffClient(&conf, replicaConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start client: %v\n", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())

	log.Println(`client start...`)

	go func() {
		if conf.ExitAfter > 0 {
			select {
			case <-time.After(time.Duration(conf.ExitAfter) * time.Second):
			case <-signals:
				log.Println("Exiting...")
			}
		} else {
			<-signals
			log.Println("Exiting...")
		}
		cancel()
	}()

	err = client.SendViewChangeCommands(ctx, *prepareNum, *viewchangeNum)
	if err != nil && !errors.Is(err, io.EOF) {
		fmt.Fprintf(os.Stderr, "Failed to send commands: %v\n", err)
		client.Close()
		os.Exit(1)
	}
	client.Close()
}

type qspec struct {
	faulty int
	n      int
}

func (q *qspec) ExecCommandQF(_ *client.Command, signatures map[uint32]*client.Empty) (*client.Empty, bool) {
	if len(signatures) < q.faulty+1 { // 对于SBFT来说，这里设为1即可，还是按照gorum的写法即可。
		return nil, false
	}
	return &client.Empty{}, true
}

func (q *qspec) AskViewChangeQF(_ *client.Empty, signatures map[uint32]*client.Empty) (*client.Empty, bool) {
	if len(signatures) < q.n {
		return nil, false
	}
	return &client.Empty{}, true
}

func (q *qspec) RoundTripQF(_ *client.Empty, signatures map[uint32]*client.Empty) (*client.Empty, bool) {
	if len(signatures) < q.n {
		return nil, false
	}
	return &client.Empty{}, true
}

type hotstuffClient struct {
	inflight      uint64
	reader        io.ReadCloser
	conf          *options
	mgr           *client.Manager
	replicaConfig *config.ReplicaConfig
	gorumsConfig  *client.Configuration
	wg            sync.WaitGroup
	stats         benchmark.Stats       // records latency and throughput
	data          *client.BenchmarkData // stores time and duration for each command
}

func newHotStuffClient(conf *options, replicaConfig *config.ReplicaConfig) (*hotstuffClient, error) {
	nodes := make(map[string]uint32, len(replicaConfig.Replicas))
	for _, r := range replicaConfig.Replicas {
		nodes[r.Address] = uint32(r.ID)
	}

	grpcOpts := []grpc.DialOption{grpc.WithBlock()}

	if conf.TLS {
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(replicaConfig.CertPool, "")))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	mgr, err := client.NewManager(client.WithNodeMap(nodes), client.WithGrpcDialOptions(grpcOpts...),
		client.WithDialTimeout(time.Minute),
	)
	if err != nil {
		return nil, err
	}
	faulty := (len(replicaConfig.Replicas) - 1) / 3
	gorumsConf, err := mgr.NewConfiguration(mgr.NodeIDs(), &qspec{faulty: faulty, n: len(replicaConfig.Replicas)})
	if err != nil {
		mgr.Close()
		return nil, err
	}
	var reader io.ReadCloser
	if conf.DataSource == "" {
		reader = ioutil.NopCloser(rand.Reader)
	} else {
		f, err := os.Open(conf.DataSource)
		if err != nil {
			mgr.Close()
			return nil, err
		}
		reader = f
	}
	return &hotstuffClient{
		reader:        reader,
		conf:          conf,
		mgr:           mgr,
		replicaConfig: replicaConfig,
		gorumsConfig:  gorumsConf,
		data:          &client.BenchmarkData{},
	}, nil
}

func (c *hotstuffClient) Close() {
	c.mgr.Close()
	c.reader.Close()
}

func (c *hotstuffClient) SendViewChangeCommands(ctx context.Context, prepareNum int, viewchangeNum int) error {

	// 先获取round-trip的时间
	roundtripNum := 1000
	var rrtTotal time.Duration
	var i int
	for i = 0; i < roundtripNum; i++ {
		start := time.Now()
		promise := c.gorumsConfig.RoundTrip(ctx, &client.Empty{})
		_, err := promise.Get()
		duration := time.Since(start)
		rrtTotal = rrtTotal + duration
		if err != nil {
			qcError, ok := err.(client.QuorumCallError)
			if !ok || qcError.Reason != context.Canceled.Error() {
				log.Printf("Did not get enough signatures for command: %v\n", err)
			}
			break
		}
	}
	rtt := float64(rrtTotal.Milliseconds()) / float64(i)
	log.Printf("round trip time: %vms\n", rtt)

	for i := 1; i <= prepareNum; i++ {
		cmd := &client.Command{
			ClientID:       uint32(c.conf.SelfID),
			SequenceNumber: uint64(i),
			PayloadSize:    int32(c.conf.PayloadSize),
			// Data:           data[:n],
		}

		promise := c.gorumsConfig.ExecCommand(ctx, cmd)

		c.wg.Add(1)
		go func(promise *client.FutureEmpty) {
			_, err := promise.Get()
			if err != nil {
				qcError, ok := err.(client.QuorumCallError)
				if !ok || qcError.Reason != context.Canceled.Error() {
					log.Printf("Did not get enough signatures for command: %v\n", err)
				}
			}
			c.wg.Done()
		}(promise)
	}
	c.wg.Wait()
	if prepareNum > 0 {
		time.Sleep(3 * time.Second) // 等待server集群稳定
	}
	log.Printf("log replication for %d entries complete, start view change...\n", prepareNum)

	var totalDuration time.Duration
	for i = 0; i < viewchangeNum; i++ {
		start := time.Now()
		promise := c.gorumsConfig.AskViewChange(ctx, &client.Empty{})
		_, err := promise.Get()
		duration := time.Since(start)
		totalDuration = totalDuration + duration
		if err != nil {
			qcError, ok := err.(client.QuorumCallError)
			if !ok || qcError.Reason != context.Canceled.Error() {
				log.Printf("Did not get enough signatures for command: %v\n", err)
			}
			break
		}
	}

	log.Printf("prepare num: %d, view change num: %d, view change average time: %vms\n", prepareNum, i, float64(totalDuration.Milliseconds())/float64(i))
	return nil
}
