
package main

import (
    //"time"
    //"strings"
    //"encoding/base64"
    //"encoding/hex"
    //"os"
    //"crypto/md5"
    "crypto/rand"


    //"github.com/plan-tools/go-plan/plan"
    "github.com/plan-tools/go-plan/pnode"


	"log"
	"net"

    "github.com/plan-tools/go-plan/pservice"

    "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
    
)








/*

func (S *TestSession) MakeChannel( inTitle string ) *plan.ChannelID {


    channelID := md5.Sum( []byte( inTitle ) )

    entry := new( plan.PDIEntry )
    entry.PDIEntryCrypt = new ( plan.PDIEntryCrypt )

    entry.Header = new( plan.PDIEntryHeader )
    entry.Header.Time = plan.Time( time.Now().Unix() )
    entry.Header.Verb = plan.PDIEntryVerbChannelAdmin
    copy( entry.Header.ChannelID[:], channelID[4:] )
    entry.Header.Author = S.info.UserID
    //entry.Header.AccessChannelID

    return nil
}
*/






// server is used to implement helloworld.GreeterServer.
type server struct{}



func main() {

    pn := NewPnode()

    // Set config defaults
    err := pn.Init()
    if err != nil {
        log.Fatalf( "init failed: %v", err )
    }

    // Until there's a util to make a new community....
    if ( false ) {
        pn.CreateNewCommunity( "PLAN Foundation" )
    }

    // For each community repo a pnode is hosting, start service for that community repo.
    // By "start service" we mean that each PDI layer that repo is configured with starts up (e.g. Ethereum private distro) such that:
    //     (a) new PDI entries *from* that layer are handed over process processing
    //     (b) newly authored entried from the PLAN client are handed over to the PDI layer to be replicated to other pnodes 
    //         also carrying that community.
    for _, CR := range pn.CRbyID {
        CR.StartService()
    }

    {
        lis, err := net.Listen( pn.config.GrpcNetworkName, pn.config.GrpcNetworkAddr )
        if err != nil {
            log.Fatalf( "failed to listen: %v", err )
        }
        grpcServer := grpc.NewServer()
        pservice.RegisterPserviceServer( grpcServer, pn )
        
        // Register reflection service on gRPC server.
        reflection.Register( grpcServer )
        if err := grpcServer.Serve( lis ); err != nil {
            log.Fatalf( "failed to serve: %v", err )
        }
    }
}





/*



}

    {

        os.Remove("./foo.db")

        db, err := sql.Open("sqlite3", "./foo.db")
        if err != nil {
            log.Fatal(err)
        }
        defer db.Close()

        sqlStmt := `
        CREATE TABLE IF NOT EXISTS ChannelEntries (
            timestamp       BIGINT,
            hashname        TINYTEXT,
            tome_file_pos   INT,
            tome_entry_len  INT          
        );
        CREATE INDEX IF NOT EXISTS EntryLookup ON ChannelEntries(timestamp);
        `

        _, err = db.Exec(sqlStmt)
        if err != nil {
            log.Printf("%q: %s\n", err, sqlStmt)
            return
        }

        tx, err := db.Begin()
        if err != nil {
            log.Fatal(err)
        }
        stmt, err := tx.Prepare("insert into foo(id, name) values(?, ?)")
        if err != nil {
            log.Fatal(err)
        }
        defer stmt.Close()
        for i := 0; i < 100; i++ {
            _, err = stmt.Exec(i, fmt.Sprintf("こんにちわ世界%03d", i))
            if err != nil {
                log.Fatal(err)
            }
        }
        tx.Commit()

        rows, err := db.Query("select id, name from foo")
        if err != nil {
            log.Fatal(err)
        }
        defer rows.Close()
        for rows.Next() {
            var id int
            var name string
            err = rows.Scan(&id, &name)
            if err != nil {
                log.Fatal(err)
            }
            fmt.Println(id, name)
        }
        err = rows.Err()
        if err != nil {
            log.Fatal(err)
        }

        stmt, err = db.Prepare("select name from foo where id = ?")
        if err != nil {
            log.Fatal(err)
        }
        defer stmt.Close()
        var name string
        err = stmt.QueryRow("3").Scan(&name)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println(name)

        _, err = db.Exec("delete from foo")
        if err != nil {
            log.Fatal(err)
        }

        _, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
        if err != nil {
            log.Fatal(err)
        }

        rows, err = db.Query("select id, name from foo")
        if err != nil {
            log.Fatal(err)
        }
        defer rows.Close()
        for rows.Next() {
            var id int
            var name string
            err = rows.Scan(&id, &name)
            if err != nil {
                log.Fatal(err)
            }
            fmt.Println(id, name)
        }
        err = rows.Err()
        if err != nil {
            log.Fatal(err)
        }

    }

    var mu sync.RWMutex
    var items = make(map[string][]byte)
    go log.Printf("started server at %s", addr)
    err := redcon.ListenAndServe(addr,
        func(conn redcon.Conn, cmd redcon.Command) {
            switch strings.ToLower(string(cmd.Args[0])) {
            default:
                conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
            case "echo":
                if len(cmd.Args) != 2 {
                    conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                    return
                }
                conn.WriteBulk(cmd.Args[1])
            case "detach":
                hconn := conn.Detach()
                log.Printf("connection has been detached")
                go func() {
                    defer hconn.Close()
                    hconn.WriteString("OK")
                    hconn.Flush()
                }()
                return
            case "ping":
                conn.WriteString("PONG")
            case "quit":
                conn.WriteString("OK")
                conn.Close()
            case "set":
                if len(cmd.Args) != 3 {
                    conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                    return
                }
                mu.Lock()
                items[string(cmd.Args[1])] = cmd.Args[2]
                mu.Unlock()
                conn.WriteString("OK")
            case "get":
                if len(cmd.Args) != 2 {
                    conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                    return
                }
                mu.RLock()
                val, ok := items[string(cmd.Args[1])]
                mu.RUnlock()
                if !ok {
                    conn.WriteNull()
                } else {
                    conn.WriteBulk(val)
                }
            case "del":
                if len(cmd.Args) != 2 {
                    conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
                    return
                }
                mu.Lock()
                _, ok := items[string(cmd.Args[1])]
                delete(items, string(cmd.Args[1]))
                mu.Unlock()
                if !ok {
                    conn.WriteInt(0)
                } else {
                    conn.WriteInt(1)
                }
            }
        },
        func(conn redcon.Conn) bool {
            // use this function to accept or deny the connection.
            // log.Printf("accept: %s", conn.RemoteAddr())
            return true
        },
        func(conn redcon.Conn, err error) {
            // this is called when the connection has been closed
            // log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
        },
    )
    if err != nil {
        log.Fatal(err)
    }
}

*/

