package main

import (
	"flag"
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

	params := repo.HostParams{
		BasePath:     *flag.String("data",          "~/__PLAN.pnode",                        "Specifies the path for all file access and storage"),
		HostGrpcPort: *flag.Int   ("port",          int(repo.Const_DefaultGrpcServicePort),  "Sets the port used to bind the Repo service"),
	}

	host, err := repo.NewHost(params)
	if err != nil {
		log.Fatal(err)
    }
    
    err = host.Start()
    hostCtx := host.Ctx()
    if err != nil {
        hostCtx.Fatalf("failed to start: %v", err)
    }

	{
        go func() {
            ticker := time.NewTicker(5 * time.Second) 
            debugAbort := int(0)
            for debugAbort == 0 {
                tick := <-ticker.C
                if tick.IsZero() {
                    debugAbort = 1
                }
            }
            hostCtx.CtxStop("debug stop", nil)
        }()

		hostCtx.AttachInterruptHandler()
		hostCtx.CtxWait()
	}

	klog.Flush()
}
