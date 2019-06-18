package main

import (
	//"fmt"
	"flag"
	"log"

	"github.com/plan-systems/go-plan/plan"
)

func main() {

	init 	:= flag.Bool(	"init", 	false, 							"Creates <datadir> as a fresh/new pnode datastore")
	dataDir := flag.String(	"datadir",	"~/_PLAN_pnode", 				"Specifies the path for all file access and storage")
	port    := flag.String(	"port",	    plan.DefaultRepoServicePort, 	"Sets the port used to bind the Repo service")

	flag.Parse()
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")

	pn, err := NewPnode(*dataDir, *init, *port)
	if err != nil {
		log.Fatal(err)
	}

	{
		err := pn.Startup()
		if err != nil {
			pn.Fatalf("failed to startup pnode")
		} else {
			pn.AttachInterruptHandler()
			pn.CtxWait()
		}
	}
}