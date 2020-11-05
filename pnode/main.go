package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/plan-systems/klog"
	"github.com/plan-systems/plan-go/repo"
)

func main() {

	flag.Parse()
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")

	fset := flag.NewFlagSet("", flag.ContinueOnError)
	klog.InitFlags(fset)
	fset.Set("logtostderr", "true")
	fset.Set("v", "2")
	klog.SetFormatter(&klog.FmtConstWidth{
		FileNameCharWidth: 24,
		UseColor:          true,
	})

	klog.Flush()

	hostGrpcPort := *flag.Int("port", int(repo.Const_DefaultGrpcServicePort), "Sets the port used to bind the Repo service")
	params := repo.HostParams{
		BasePath: *flag.String("data", "~/__PLAN.pnode", "Specifies the path for all file access and storage"),
	}

	host, err := repo.NewHost(params)
	if err != nil {
		log.Fatal(err)
	}

	err = host.Start()
	if err != nil {
		host.Ctx().Fatalf("failed to start: %v", err)
	}

    // Little known fact: using "127.0.0.1" specifically binds to localhost, so incoming outside connections will be refused.
    // Later, when pnode is embedded in the client app for each platform, we'll want to use 127.0.0.1 (or IPC).
    // Until then, we want to need to accept incoming outside connections.
	srv := repo.NewGrpcServer(host, "tcp", fmt.Sprintf("0.0.0.0:%v", hostGrpcPort))
	err = srv.Start()
	if err != nil {
		srv.Fatalf("failed to start: %v", err)
	}

	// Run the grpc as the parent ctx since we only want to insert a graceful stop THEN stop the host.
	srv.CtxAddChild(host.Ctx(), nil)

	if err == nil {
		srv.AttachInterruptHandler()

		go func() {
			ticker := time.NewTicker(10 * time.Second)
			debugAbort := int(0)
			for debugAbort == 0 {
				tick := <-ticker.C
				//srv.CtxPrintDebug();
				if tick.IsZero() {
					debugAbort = 1
				}
			}
			srv.CtxPrintDebug()
			srv.CtxStop("debug stop", nil)
		}()

		srv.CtxWait()
	}

	klog.Flush()
}
