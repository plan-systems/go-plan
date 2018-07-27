
package main

import (
    "time"
    //"strings"
    //"encoding/base64"
    "encoding/hex"
    //"os"
    "crypto/md5"

    "github.com/plan-tools/go-plan/plan"
    
)






type TestSession struct {
    info            ClientInfo

    api             PnodeAPI
    keys            map[plan.CommunityKeyID]byte;

}

var gSessionsReady chan *TestSession


var (
    communutyKey1 = plan.CommunityKeyID{ 1, 1, }
    communutyKey2 = plan.CommunityKeyID{ 2, 2, }
    communutyKey3 = plan.CommunityKeyID{ 3, 3, }
)

func (S *TestSession) ClientInfo() *ClientInfo {
    return &S.info;
}




func (S *TestSession) OnLogin( inAPI PnodeAPI ) {

    user, _ := hex.DecodeString( "00112233445566778899aabbccddeeff44332211" )
    copy( S.info.UserID[:], user )
    S.api = inAPI

    // "Load" community keys
    S.keys = make( map[plan.CommunityKeyID]byte )
    S.keys[communutyKey1] = 0x01
    S.keys[communutyKey2] = 0x02
    S.keys[communutyKey3] = 0x03

    gSessionsReady <- S

}

func (S *TestSession) OnLogout() {

    // "Unload" key store from memory
    S.keys = nil
}


func (S *TestSession) DecryptCommunityData( inKeyID plan.CommunityKeyID, inEncrypted []byte ) ( []byte, error ) {

    d := make( []byte, len(inEncrypted))
    k := S.keys[inKeyID]

    for i, c := range inEncrypted {
        d[i] = c ^ k
    }

    return d, nil
}


func (S *TestSession) EncryptCommunityData( inKeyID plan.CommunityKeyID, inData []byte ) ( []byte, error ) {

    d := make( []byte, len(inData))
    k := S.keys[inKeyID]

    for i, c := range inData {
        d[i] = c ^ k
    }

    return d, nil

}






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




var addr = "127.0.0.1:6380"

func main() {

    // Normally, pnode is already running and we connect to it via tcp.  
    // Until then, pnode is launch as a Go thread inside our test environment
    go PNode_main()
    time.Sleep(1000 * time.Millisecond)

    var session TestSession

    PNode_StartFakeSession( "PLAN Foundation", &session )


    //chID := session.MakeChannel( "yoyo" )

    //for session := range gSessionsReady {

        // MakeChannel
        //S.api
    //}

    // Wait forever
    c := make(chan struct{})
    <-c


/*


func (CR *CommunityRepo) ( inEntry *plan.PDIEntryCrypt ) {

    entry = new( plan.PDIEntryCrypt )

    entry.
    err = rlp.DecodeBytes( entry.HeaderBuf, entry.Header )


    // Convert an entry header in 
    // To publish an entry, we first package it, then insert it into the repo
    CR.entriesToProcess <- entry



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
    }*/
}

