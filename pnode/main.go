package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/plan-systems/klog"
	"github.com/pkg/errors"
	"github.com/urfave/cli"

	"github.com/plan-systems/plan-go/ctx"
	"github.com/plan-systems/plan-go/repo"

       "github.com/plan-systems/redwood-go/tree"
	rw "github.com/plan-systems/redwood-go"
)

var app = struct {
	ctx.Context
}{}

func main() {
	cliApp := cli.NewApp()
	// cliApp.Version = env.AppVersion

	configRoot, err := defaultConfigRoot()
	if err != nil {
		app.Error(err)
		os.Exit(1)
	}

	cliApp.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config",
			Value: filepath.Join(configRoot, ".planrc"),
			Usage: "location of config file",
		},
		cli.BoolFlag{
			Name:  "pprof",
			Usage: "enable pprof",
		},
		// cli.BoolFlag{
		// 	Name:  "gui",
		// 	Usage: "enable CLI GUI",
		// },
	}

	cliApp.Action = func(c *cli.Context) error {
		configPath := c.String("config")
		// gui := c.Bool("gui")
		enablePprof := c.Bool("pprof")
		return run(configPath, enablePprof)
	}

	err = cliApp.Run(os.Args)
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
}

func defaultConfigRoot() (root string, _ error) {
	configRoot, err := os.UserConfigDir()
	if err != nil {
		configRoot, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}
	configRoot = filepath.Join(configRoot, "plan")
	return configRoot, nil
}

func run(configPath string, enablePprof bool) error {
	if enablePprof {
		go func() {
			http.ListenAndServe(":6060", nil)
		}()
		runtime.SetBlockProfileRate(int(time.Millisecond.Nanoseconds()) * 100)
	}

	// var termUI *termUI
	// if gui {
	// 	termUI = NewTermUI()
	// 	go termUI.Start()
	// 	defer termUI.Stop()

	// 	flagset := flag.NewFlagSet("", flag.ContinueOnError)
	// 	klog.InitFlags(flagset)
	// 	flagset.Set("v", "2")
	// 	flagset.Set("log_file", "/tmp/asdf") // This is necessary to keep the logger in "single mode" -- otherwise logs will be duplicated
	// 	klog.SetOutput(termUI.LogPane)
	// 	klog.SetFormatter(&FmtConstWidth{
	// 		FileNameCharWidth: 24,
	// 		UseColor:          true,
	// 	})

	// } else {
	flagset := flag.NewFlagSet("", flag.ContinueOnError)
	klog.InitFlags(flagset)
	flagset.Set("logtostderr", "true")
	flagset.Set("v", "2")
	klog.SetFormatter(&klog.FmtConstWidth{
		FileNameCharWidth: 24,
		UseColor:          true,
	})
	// }

	klog.Flush()
	defer klog.Flush()

	config, err := rw.ReadConfigAtPath(configPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(filepath.Dir(configPath), 0600)
		if err != nil {
			return err
		}
		config, err = rw.ReadConfigAtPath(configPath)
		if err != nil {
			return err
		}

	} else if err != nil {
		return err
	}

	err = ensureDataDirs(config)
	if err != nil {
		return err
	}

	signingKeypair, err := rw.SigningKeypairFromHDMnemonic(config.Node.HDMnemonicPhrase, rw.DefaultHDDerivationPath)
	if err != nil {
		return err
	}

	encryptingKeypair, err := rw.GenerateEncryptingKeypair()
	if err != nil {
		return err
	}

	var (
		txStore       = rw.NewBadgerTxStore(config.TxDBRoot())
		refStore      = rw.NewRefStore(config.RefDataRoot())
		peerStore     = rw.NewPeerStore()
		controllerHub = rw.NewControllerHub(signingKeypair.Address(), config.StateDBRoot(), txStore, refStore)
	)

	err = refStore.Start()
	if err != nil {
		return err
	}
	app.CtxAddChild(refStore.Ctx(), nil)

	err = txStore.Start()
	if err != nil {
		return err
	}
	app.CtxAddChild(txStore.Ctx(), nil)

	var transports []rw.Transport

	if config.P2PTransport.Enabled {
		libp2pTransport, err := rw.NewLibp2pTransport(
			signingKeypair.Address(),
			config.P2PTransport.ListenPort,
			config.P2PTransport.KeyFile,
			encryptingKeypair,
			controllerHub,
			refStore,
			peerStore,
		)
		if err != nil {
			return err
		}
		transports = append(transports, libp2pTransport)
	}

	if config.HTTPTransport.Enabled {
		tlsCertFilename := filepath.Join(config.Node.DataRoot, "server.crt")
		tlsKeyFilename := filepath.Join(config.Node.DataRoot, "server.key")

		var cookieSecret [32]byte
		copy(cookieSecret[:], []byte(config.HTTPTransport.CookieSecret))

		httpTransport, err := rw.NewHTTPTransport(
			config.HTTPTransport.ListenHost,
			config.HTTPTransport.DefaultStateURI,
			controllerHub,
			refStore,
			peerStore,
			signingKeypair,
			encryptingKeypair,
			cookieSecret,
			tlsCertFilename,
			tlsKeyFilename,
			config.Node.DevMode,
		)
		if err != nil {
			return err
		}
		transports = append(transports, httpTransport)
	}

	host, err := rw.NewHost(signingKeypair, encryptingKeypair, transports, controllerHub, refStore, peerStore, config)
	if err != nil {
		return err
	}

	RepoServer := repo.NewGrpcServer(host, "tcp", "127.0.0.1:5192")

	host.Ctx().CtxAddChild(RepoServer, nil)

	err = RepoServer.Start()
	if err != nil {
		return err
	}

	err = host.Start()
	if err != nil {
		return err
	}

	for _, bootstrapPeer := range config.Node.BootstrapPeers {
		bootstrapPeer := bootstrapPeer
		go func() {
			app.Infof(0, "connecting to bootstrap peer: %v %v", bootstrapPeer.Transport, bootstrapPeer.DialAddresses)
			for _, dialAddr := range bootstrapPeer.DialAddresses {
				host.AddPeer(rw.PeerDialInfo{TransportName: bootstrapPeer.Transport, DialAddr: dialAddr})
			}
		}()
	}

	go func() {
		time.Sleep(5 * time.Second)
		app.Warnf("trying to subscribe %+v", config.Node.SubscribedStateURIs)
		for stateURI := range config.Node.SubscribedStateURIs {
			app.Warnf("trying to subscribe to %v", stateURI)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

            sub, err := host.Subscribe(ctx, stateURI)
			if err != nil {
				host.Errorf("error subscribing to URI %v: %v", stateURI, err)
			}
            sub.Close() // We don't need the in-process subscription
			app.Infof(1, "subscribed to %v", stateURI)
		}
	}()

	app.CtxAddChild(host.Ctx(), nil)
	app.CtxStart(
		func() error { return nil },
		nil,
		nil,
		nil,
	)
	defer app.CtxStop("shutdown", nil)

	klog.Info(rw.PrettyJSON(config))

	// if gui {
	// 	go func() {
	// 		for {
	// 			select {
	// 			case <-time.After(3 * time.Second):
	// 				stateURIs, err := host.Controllers().KnownStateURIs()
	// 				if err != nil {
	// 					continue
	// 				}

	// 				termUI.Sidebar.SetStateURIs(stateURIs)
	// 				states := make(map[string]string)
	// 				for _, stateURI := range stateURIs {
	// 					node, err := host.Controllers().StateAtVersion(stateURI, nil)
	// 					if err != nil {
	// 						panic(err)
	// 					}
	// 					states[stateURI] = rw.PrettyJSON(node)
	// 				}
	// 				termUI.StatePane.SetStates(states)
	// 			case <-termUI.Done():
	// 				return
	// 			}
	// 		}
	// 	}()
	// 	<-termUI.Done()

	// } else {
	go inputLoop(host)
	app.AttachInterruptHandler()
	app.CtxWait()
	// }
	return nil
}

func ensureDataDirs(config *rw.Config) error {
	err := os.MkdirAll(config.RefDataRoot(), 0700)
	if err != nil {
		return err
	}

	err = os.MkdirAll(config.TxDBRoot(), 0700)
	if err != nil {
		return err
	}

	err = os.MkdirAll(config.StateDBRoot(), 0700)
	if err != nil {
		return err
	}
	return nil
}

func inputLoop(host rw.Host) {
	fmt.Println("Type \"help\" for a list of commands.")
	fmt.Println()

	var longestCommandLength int
	for cmd := range replCommands {
		if len(cmd) > longestCommandLength {
			longestCommandLength = len(cmd)
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("> ")

		if !scanner.Scan() {
			break
		}

		line := scanner.Text()
		parts := strings.Split(line, " ")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}

		if len(parts) < 1 {
			app.Error("enter a command")
			continue
		} else if parts[0] == "help" {
			fmt.Println("___ Commands _________")
			fmt.Println()
			for cmd, info := range replCommands {
				difference := longestCommandLength - len(cmd)
				space := strings.Repeat(" ", difference+4)
				fmt.Printf("%v%v- %v\n", cmd, space, info.HelpText)
			}
			continue
		}

		cmd, exists := replCommands[parts[0]]
		if !exists {
			app.Error("unknown command")
			continue
		}

		err := cmd.Handler(app.Ctx(), parts[1:], host)
		if err != nil {
			app.Error(err)
		}
	}
}

var replCommands = map[string]struct {
	HelpText string
	Handler  func(ctx context.Context, args []string, host rw.Host) error
}{
	"stateuris": {
		"list all known state URIs",
		func(ctx context.Context, args []string, host rw.Host) error {
			stateURIs, err := host.Controllers().KnownStateURIs()
			if err != nil {
				return err
			}
			if len(stateURIs) == 0 {
				fmt.Println("no known state URIs")
			} else {
				for _, stateURI := range stateURIs {
					fmt.Println("- ", stateURI)
				}
			}
			return nil
		},
	},
	"state": {
		"print the current state tree",
		func(ctx context.Context, args []string, host rw.Host) error {
			if len(args) < 1 {
				return errors.New("missing argument: state URI")
			}

			stateURI := args[0]
			state, err := host.Controllers().StateAtVersion(stateURI, nil)
			if err != nil {
				return err
			}
			var keypath tree.Keypath
			var rng *tree.Range
			if len(args) > 1 {
				_, keypath, rng, err = rw.ParsePatchPath([]byte(args[1]))
				if err != nil {
					return err
				}
			}
			app.Debugf("stateURI: %v / keypath: %v / range: %v", stateURI, keypath, rng)
			state = state.NodeAt(keypath, rng)
			state.DebugPrint(app.Debugf, false, 0)
			fmt.Println(rw.PrettyJSON(state))
			return nil
		},
	},
}
