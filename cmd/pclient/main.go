package main

import (
	//"fmt"
	//"context"
	"flag"
	//"io/ioutil"
	"log"
	//"os"

	//"time"
	//"io"
	//"bufio"
	//"bytes"
	//"fmt"

	//"path"
	//"strconv"
	//"sync"
	//"sync/atomic"

	//"github.com/plan-systems/go-plan/client"
	//"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	//"github.com/plan-systems/go-plan/repo"

)

const localRepo = ":" + plan.DefaultRepoServicePort

func main() {

	init 		:= flag.Bool("init", 		false, 				"init creates <datadir> as a fresh/new pclient datastore")
	dataDir 	:= flag.String("datadir", 	"~/_PLAN_pclient", 	"datadir specifies the path for all file access and storage")
	seed 		:= flag.String("seed", 		"", 				"seed reads a PLAN seed file ands seeds a community instance on the host pnode")
	port    	:= flag.String("port",	    plan.DefaultWorkstationServicePort, "Sets the port used to bind the Repo service")
	repoAddr 	:= flag.String("repoAddr",	localRepo, 			"host specifies the net addr of a host pnode for all connection")
	//testRepeat  := flag.String("Trepeat",    "10",				"how often a test msg will be posted to the test channel")
	//testCreate  := flag.String("Topen",    	"", 				"opens the given channel (vs creating a new channel)")

	flag.Parse()
	flag.Set("logtostderr", "true")
	flag.Set("v", "2")

	ws, err := NewWsHost(
		*dataDir, 
		*init,
		*port,
		*repoAddr,
	)
	if err != nil {
		log.Fatal(err)
	}

	if len(*seed) > 0 {
		_, err = ws.CreateNewSeatFromSeedFile(*seed)
	}
	if err != nil {
		log.Fatal(err)
	}

	err = ws.Startup()
	if err != nil {
		log.Fatalf("failed to startup: %v", err)
	}

	ws.AttachInterruptHandler()


	/*
	{
		seedMember := false
		if len(*seed) > 0 {
			_, err = ws.ImportSeed(*seed)
			seedMember = true
		}
		if err != nil {
			log.Fatal(err)
		}

		{
			err := ws.Startup()
			if err != nil {
				log.Fatalf("failed to startup: %v", err)
			}

			ws.AttachInterruptHandler()

			err = ws.Login(*hostAddr, 0, seedMember)
			if err != nil {
				log.Fatalf("client-sim fatal err: %v", err)
			}
		}

		{
			testDelay := float64(10)
			if testDelay, err = strconv.ParseFloat(*testRepeat, 32); err != nil {
				log.Fatal(err)
			}

			var openChID []byte
			if testCreate != nil && len(*testCreate) > 0 {
				if openChID, err = plan.BinEncoding.DecodeString(*testCreate); err != nil {
					log.Fatal(err)
				}
			}
			if len(openChID) != 0 && len(openChID) != plan.ChIDSz {
				log.Fatal("bad channel ID")
			}

			reader := bufio.NewReader(os.Stdin)
			fmt.Println("ENTER TO START")
			reader.ReadString('\n')

			N := 1
			//waiter := sync.WaitGroup
			//time.Sleep(5 * time.Second)

			for i := 0; i < N; i++ {
				go doTest(ws.ms, openChID, int64(testDelay*1000))
			}

		}
	}*/

	ws.CtxWait()
}


/*
func doTest(
	ms *RepoSess,
	chID plan.ChID,
	repeatMS int64,
) {

	var (
		//cs *chSess
		//err error
	)
	isRdy := make(chan *chSess, 1)
	if len(chID) == plan.ChIDSz {
		ms.Infof(0, "opening channel %s", chID.Str())
		cs, err := ms.openChannel(chID)
		if err != nil {
			log.Fatal(err)
		}

		isRdy <- cs
	} else {
		ms.createNewChannel(repo.ChProtocolTalk, func(
			inCS *chSess,
			inErr error,
		) {
			if inErr != nil {
				ms.Error("createNewChannel error: ", inErr)
			} else {
				ms.Infof(0, "created new channel %s", inCS.ChID.Str())
			}

			isRdy <- inCS
		})
	}

	cs :=  <- isRdy
	if cs == nil {
		return
	}

	cs.resetReader()

	for i := 1; ms.CtxRunning(); i++ {
		hello := fmt.Sprintf("Hello, Universe #%d, from %s", i, path.Base(ms.ws.BasePath))

		cs.PostContent(
			[]byte(hello),
			func(
				inEntryInfo *pdi.EntryInfo,
				inEntryState *repo.EntryState,
				inErr error,
			) {
				if inErr != nil {
					fmt.Println("PostContent() -- got commit hello err:", inErr)
				} else {
					//fmt.Println("PostContent() entry ID", inEntryInfo.EntryID().SuffixStr())
				}
			},
		)
		time.Sleep(time.Duration(repeatMS) * time.Millisecond)
	}
}





// REMOVE ME

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("ENTER TO START")
	reader.ReadString('\n')

	N := 1
	//waiter := sync.WaitGroup
	//time.Sleep(5 * time.Second)

	for i := 0; i < N; i++ {
		idx := i
		go func() {
			ms.Infof(0, "creating new channel %d", idx)
			ms.createNewChannel(repo.ChProtocolTalk, func(
				cs *chSess,
				inErr error,
			) {
				if err != nil {
					fmt.Print("createNewChannel error: ", inErr)
				} else {
					for i := 1; i < 100000; i++ {
						hello := fmt.Sprintf("Hello, Universe!  This is msg #%d in channel %s", i, cs.ChID.SuffixStr())
						//fmt.Println("=======> ", hello)

						if i == 6 {
							cs.readerTest()
						}

						cs.PostContent(
							[]byte(hello),
							func(
								inEntryInfo *pdi.EntryInfo,
								inEntryState *repo.EntryState,
								inErr error,
							) {
								if inErr != nil {
									fmt.Println("PostContent() -- got commit hello err:", inErr)
								} else {
									fmt.Println("PostContent() entry ID", inEntryInfo.EntryID().SuffixStr())
								}
							},
						)
						time.Sleep(5050 * time.Millisecond)

					}
				}
			})
		}()
	}

	reader.ReadString('\n')

	time.Sleep(1000 * time.Second)

	return nil
}
*/