
package main

import (
	"database/sql"
	"fmt"
	"log"
    "os"
    "io"
	"strings"
    "sync"
    "time"
    //"sort"
    "encoding/hex"

    "github.com/tidwall/redcon"

    "github.com/plan-tools/go-plan/plan"

	/*
	   multihash       "github.com/multiformats/go-multihash"
	   multibase       "github.com/multiformats/go-multibase"
	   multistream     "github.com/multiformats/go-multistream"
	   multicodec      "github.com/multiformats/go-multicodec"
    */
    
    _ "github.com/mattn/go-sqlite3"
    
    //"crypto/md5"
    //"hash"
    "encoding/base64"

    //"github.com/stretchr/testify/assert"
)


const (
    DEBUG     = true
)



type pnodeGlobals struct {
	repos                  map[*plan.IdentityAddr]*CommunityRepo
}





func init() {

}


type ClientInfo struct {

    UserId                  plan.IdentityAddr        // Creator of this entry (and signer of .Sig)

}

type ClientSession interface {
    ClientInfo() *ClientInfo

    OnLogin()
    OnLogout()

    // Encrypts/Decrypts the given data, given a community key id.
    DecryptCommunityData( inCommunityKey plan.AccessCtrlId, inEncrypted []byte ) ( []byte, error )
    EncryptCommunityData( inCommunityKey plan.AccessCtrlId, inData      []byte ) ( []byte, error )
}


type FakeSession struct {

    keys             map[plan.AccessCtrlId]byte;

}



var (
    CommunutyKey1 = plan.AccessCtrlId{ 1, 1, }
    CommunutyKey2 = plan.AccessCtrlId{ 2, 2, }
    CommunutyKey3 = plan.AccessCtrlId{ 3, 3, }
)

func (S *FakeSession) OnLogin() {

    // "Load" community keys
    S.keys = make( map[plan.AccessCtrlId]byte )
    S.keys[CommunutyKey1] = 0x01
    S.keys[CommunutyKey2] = 0x02
    S.keys[CommunutyKey3] = 0x03
}

func (S *FakeSession) OnLogout() {

    // "Unload" key store from memory
    S.keys = nil
}


func (S *FakeSession) DecryptCommunityData( inKeyId plan.AccessCtrlId, inEncrypted []byte ) ( []byte, error ) {

    d := make( []byte, len(inEncrypted))
    k := S.keys[inKeyId]

    for i, c := range inEncrypted {
        d[i] = c ^ k
    }

    return d, nil
}


func (S *FakeSession) EncryptCommunityData( inKeyId plan.AccessCtrlId, inData []byte ) ( []byte, error ) {

    d := make( []byte, len(inData))
    k := S.keys[inKeyId]

    for i, c := range inData {
        d[i] = c ^ k
    }

    return d, nil

}


type ChannelStoreLookup struct {
    sync.RWMutex
	table map[plan.ChannelId]*ChannelStore
}


type ACStoreLookup struct {
    sync.RWMutex
	table map[plan.AccessCtrlId]*ACStore
}


// Community Data Repository
type CommunityRepo struct {
	communityName           string
    communityAddr           plan.CommunityAddr
    clientSession           ClientSession

	basePath                string
	channelPath             string // subdir containing data for each channel
    acPath                  string // subdir containing all the access control dbs

    // Decrypted entries ready to be written to the community's common db (on local disk)
    entriesToStore          chan *plan.PDIEntry

    // Incoming encrypted entries "off the wire" that are ready to be processed
    entriesToProcess        chan *plan.PDIEntryCrypt

    dirEncoding             *base64.Encoding

    DefaultFileMode         os.FileMode

    channelStores           ChannelStoreLookup      // Turns a plan.ChannelId into a *ChannelStore
	acStores                ACStoreLookup           // Turns a plan.AccessCtrlId into an *ACStore
}



// Initializes a new channel store
// CR is read-only since this can called from any thread!
func (CR *CommunityRepo) InitChannel( inChannelId *plan.ChannelId ) *ChannelStore {

    CS := new( ChannelStore )

    CS.ChannelId    = *inChannelId    //CR.chNameHasher.Sum( []byte( NormalizeChannelName( CS.channelName ) ) )
    CS.channelDir   = CR.channelPath + CR.dirEncoding.EncodeToString( inChannelId[:] ) + "/"

    os.Mkdir( CS.channelDir, CR.DefaultFileMode )

    CR.channelStores.Lock()
    CR.channelStores.table[CS.ChannelId] = CS
    CR.channelStores.Unlock()

    return CS
}





func (CR *CommunityRepo) EntryProcessor() {


    for entry := range CR.entriesToProcess {

        err := CR.ProcessEntry( entry )
    }
}



func (CR *CommunityRepo) ProcessEntry( ioEntry *plan.PDIEntryCrypt ) error {

    var err error
    ioEntry.Hash, err = ioEntry.ComputeHash()
    if err != nil {
        return err
    }

    ioEntry.HeaderBuf, err = CR.DecryptCommunityData( ioEntry.CommunityKeyRev, ioEntry.HeaderCrypt )
    if err != nil {
        return err
    }

    // Deserialize inEntry.Headerbuf into inEntry.Header
    err = ioEntry.DeserializeHeader()
    if err != nil {
        return err
    }

    // Now that we've decrypted and deserialized the header, we can verify the entry's signature
    err = CR.VerifySig( ioEntry )
    if err != nil {
        return err
    }


    // With the header 
    info, err := ioCR.QueryAccessCtrl( ioEntry.Header.AccessCtrlId, ioEntry.Header.Author )
    if err != noErr {
        return err
    }

    if ! info.AuthorHasWriteAccess {
        return AuthorLacksWritePermission
    }

    CR.entriesToStore <- ioEntry


}

func (CR *CommunityRepo) EntryStorageWriter() {


    for entry := range CR.entriesToStore {

        CR.channelStores.RLock()
        CS := CR.channelStores.table[entry.Header.ChannelId]
        CR.channelStores.RUnlock()

        // Init/Create a channel storage handler for this channelId
        if CS == nil {
            CS = CR.InitChannel( &entry.Header.ChannelId )
        }
    
        err := CS.WriteEntryToStorage( entry )

    }
}


func (CR *CommunityRepo) DecryptCommunityData( inKeyRev plan.CommunityKeyRev, inEncrypted []byte ) ( []byte, error ) {

    return CR.clientSession.DecryptCommunityData( inKeyRev, inEncrypted )

}

func (CR *CommunityRepo) EncryptCommunityData( inKeyRev plan.CommunityKeyRev, inData []byte ) ( []byte, error ) {

    return CR.clientSession.EncryptCommunityData( inKeyRev, inData )
    
}

/*
func (CR *CommunityRepo) Sign( inEntropy io.Reader, inPrivateKey *PrivateKey, inHash []byte ) ( []byte, err error ) {

    return CR.clientSession.EncryptCommunityData( inKeyRev, inData )
    
}
*/

// inSig is the sig data block
// inHash specifies how inHashed was created (or 0 if not inHashed is not hashed)
// inHashed is the hash digest that is said to be signed by inSigner into inSig
// This function verifies that inSig a signature of inHashed from the private key associated with inSigner
func (CR *CommunityRepo) VerifySig( inEntry *plan.PDIEntry ) error {

    return nil
    
}



const (
    ChannelIndexFilename        = "idx.db"
    ChannelTomeFilename         = "01"
    ChannelAccessCtrlFilename   = "ac.db"

    kTomeNumShift               = byte( 64 - 16 )
    kTomeNumMask                = 0xFFFF << kTomeNumShift
    kTomeOffsetMask             = ( 1 << kTomeNumShift ) - 1
)

// TODO: Use file pool that auto-flushes and closes files after X seconds or as file access goes past N txns, etc.
// TODO: make an explicit command that creates a new channel -- this enforces channel case-sensitive names

type ChannelStore struct {
    ChannelId               plan.ChannelId

    channelDir              string

	acStore                 *ACStore

    db                      *sql.DB
    select_timeMatch        *sql.Stmt
    select_hashMatch        *sql.Stmt
    insert_entry            *sql.Stmt

	tome_file               *os.File
}






func (CS *ChannelStore) AppendToTome( inTomeNum int64, inBlob []byte ) ( TomePosEncoding, error ) {

    plan.Assert( inTomeNum == 1, "ChannelStore currently only implements a single-tome data file" )

    var err error
    if CS.tome_file == nil {
        filename := CS.channelDir + ChannelIndexFilename
        CS.tome_file, err = os.OpenFile( filename, os.O_APPEND | os.O_CREATE, os.ModeAppend | 0666 )
        if err != nil {
            log.Printf( "Error opening channel tome '%s'\n", filename )
            return -1, err
        }
    }

    var n int
    pos, err := CS.tome_file.Seek( 0, io.SeekCurrent )
    if err == nil {
        n, err = CS.tome_file.Write( inBlob )
    }
    if err != nil {
        log.Printf( "Error writing to channel tome: pos=%d (%d/%d bytes)'\n", pos, n, len(inBlob) )
        return -1, err
    }


    return TomePosEncoding( ( inTomeNum << byte(kTomeNumShift) ) | pos ), nil
}


func (CS *ChannelStore) Close() {

    if CS.db != nil {
        CS.db.Close()
    }

    if CS.tome_file != nil {
        CS.tome_file.Close()
    } 
}




type TomePosEncoding int64

func (CS *ChannelStore) OpenIndexDB() error {

    if CS.db == nil {
        filename := CS.channelDir + ChannelIndexFilename
        var err error
        CS.db, err = sql.Open( "sqlite3", filename )
		if err != nil {
            log.Printf( "Error opening channel index db '%s'\n", filename )
			return err
        }
    
        // Note that we don't use a hash index since we have EntryLookupByTime.
        // Since all entries have an associated timestamp, an entry can be found in O(1) time (as long as its hashname is accompanied by its timestamp,
        //     which is ok since incoming entries will always include the author timestamp.
        // This is reasonable and saves the trouble of maintaining a big index plus the lookup time.  Plus the chance of a hash collision goes down 
        //    since the collision would have to occur during the same second UTC!
        sqlStmt := `
        CREATE TABLE IF NOT EXISTS ChannelEntries (
            tsAuthor            BIGINT,         /* Unix timestamp bore by the entry itself                      */
            tsConsensus         BIGINT,         /* Best available unix timestamp to use for this entry          */
            tsRecvDelay         INT,            /* <time-entry-received> - tsAuthor                             */
            entryHash           VARBINARY(32),  /* Entry's hashname                                             */
            tome_entry_pos      BIGINT,         /* TomePosEncoding                                              */
            tome_entry_len      INT             /* Number of bytes starting at tome_entry_pos                   */
        );
        CREATE INDEX IF NOT EXISTS EntryLookup ON ChannelEntries(tsAuthor ASC);
        `
		_, err = CS.db.Exec( sqlStmt )
		if err != nil {
            log.Printf( "Error bootstrapping channel db '%s'\n", filename )
            log.Fatal( err )
			return err
        }
        
        // TODO: ANALYZE
        CS.select_timeMatch, err = CS.db.Prepare( "SELECT rowid FROM ChannelEntries WHERE tsAuthor = ?" )
		if err != nil {
			log.Fatal(err)
        }
        CS.select_hashMatch, err = CS.db.Prepare( "SELECT 55 FROM ChannelEntries WHERE rowid = ? AND entryHash = ?" )
		if err != nil {
			log.Fatal(err)
        }
      
        CS.insert_entry, err = CS.db.Prepare( "INSERT INTO ChannelEntries( tsAuthor, tsConsensus, tsRecvDelay, entryHash, tome_entry_pos, tome_entry_len ) VALUES (?,?,?,?,?,?)" )
		if err != nil {
			log.Fatal(err)
        }
        
    }

    return nil

}


// Computes a hash digest for this entry
func ( inEntry *plan.PDIEntryCrypt ) ComputeHash() *plan.PDIEntryHash {


}


func ( ioEntry *plan.PDIEntryCrypt ) HashEntry() *plan.PDIEntryHash {

    if ioEntry.Hash != nil {
        return &ioEntry.Hash
    }



}



// Inserts the given entries to the this channel
// Pre: CS.channelName == inEntries[:].data.channelName
func (CS *ChannelStore) WriteEntryToStorage( inEntry *plan.PDIEntry ) error {

    plan.Assert( inEntry.Header.ChannelId == CS.ChannelId, "Bad channel given to InsertEntriesToChannel" )


    err := CS.OpenIndexDB()
    if err != nil {
        return err
    }

    {
        var err error
        var matchRow int 

        {
            err = CS.select_timeMatch.QueryRow( inEntry.Header.Timestamp ).Scan( &matchRow )
            if err == sql.ErrNoRows {

            } else {
                log.Printf( "got possible dupe!" )

                err = CS.select_timeMatch.QueryRow( matchRow, *inEntry.Crypt.Hash ).Scan( &matchRow )

                log.Printf( "got dupe!" )
                return nil
            }
            // fix me
        }
    }

    /*
    // TODO: make faster by doing a single t_authored lookup and then stepping thru row hits looking for 
    rows, err := db.Query("SELECT rowid FROM ChannelEntries WHERE t_authored = ? AND entryHash = ?")


    exists, err := tx.Prepare( "SELECT rowid FROM ChannelEntries WHERE hashname = ?" )
    if err != nil {
        log.Fatal( err )
    }

    // all entries must be the same channel name
    if DEBUG {
        
    }*/

    t_now := time.Now().Unix()


    _, err = CS.insert_entry.Exec( t_now, 2, 3, 4, 5, 6 )
    if err != nil {
        log.Fatal( err )
    }

    return nil
        
}


type ACStore struct {
	accessCtrlId            plan.AccessCtrlId
	//accessFlags             map[string]AccessFlags
	db                      *sql.DB
}



/*



// A channel access control list (PDIEntry.accessCtrlRef)
//     - Effectively allows a composite user and group access control mapping be built (in effect specifying users and groups that have read, write, ownership permission).
//     - Maps to a keyring entry in each user's SKI containing the corresponding master key to encrypt/decrypt channel entries when that ACL was in effect
//     - Any community user can be...
//          ...issued the channel msg master key (read access)
//              - Entries contain the user's pubkey and their "verify" pubkey (allowing signatures to be authenticated)
//              - These users are issued new master keys if/when a channel moves to newly generated access control list
//          ...placed on the channel's write list (write access)
//              - Although a user could hack their client so that they're on this write list, other nodes will not have this alteration and will reject the entry.
//          ...placed on a ban list.  Similar to the a write access hack, a banned user's entry will be rejected by other swarm nodes.
//              - This allows a channel to be offer community-public write access, except for explicitly named user or group id.

type AuthorCredentials struct {
	hasReadAccess           bool
	hasWriteAccess          bool
	hasOwnership            bool
	verifyKey               string // (multibase) -- key used to verify than an entry is signed by the author it says its authored by
}

*/

/*
func (CR *CommunityRepo) AccessChannel(inChannelName string) ChannelStore {

	CR.ChannelHashFunction
}
*/



func (CR *CommunityRepo) ProcessEntryFromWire( inEntry *plan.PDIEntryCrypt ) error {


    /*
    if info.IsPublicChannel {
        ioEntry.BodyBuf, err = CR.DecryptCommunityData( ioEntry.CommunityKeyRev, ioEntry.BodyCrypt )
        if err != noErr {
            return err
        }

    } else


	if !authorCred.hasWriteAccess {
		return AuthorLacksWritePermission
	}

	authoredData, err := ioEntry.DecryptWithVerifyKey(authorCred.verifyKey)
	if err != noErr {
		return err
	}

	// yikes, below we won't be able to decrypt if we don't have access to this channel.
	// so every channel's basically stores itself in an encrypted state and decrypts on the fly

	ioEntry.data, err := CR.DecryptEntry( ioEntry, authoredData )
	if err != noErr {
		return err
	}

    err = ioCR.appendEntryData(ioEntry)
    

    if ( )
    err = CR.Decrypt */

}




func (CR *CommunityRepo) BootstrapCDS() {

	{
		var b strings.Builder
		remapCharset := map[rune]rune{
			' ':  '-',
			'.':  '-',
			'?':  '-',
			'\\': '+',
			'/':  '+',
			'&':  '+',
		}

		for _, r := range strings.ToLower(CR.communityName) {
			if replace, ok := remapCharset[r]; ok {
				if replace != 0 {
					b.WriteRune(replace)
				}
			} else {
				b.WriteRune(r)
			}
		}


		CR.basePath = b.String() + "-" //+ hex.EncodeToString( CR.address[:8] )
		CR.channelPath      = CR.basePath + "/ch/"
		CR.acPath           = CR.basePath + "/ac/"
        CR.dirEncoding      = base64.RawURLEncoding
        CR.clientSession    = new( FakeSession )
        CR.entriesToStore   = make( chan *PDIEntry,         32 )
        CR.entriesToProcess = make( chan *PDIEntryCrypt,    32 )
        CR.DefaultFileMode  = os.FileMode(0775)
	}

	os.MkdirAll(CR.basePath, CR.DefaultFileMode )

	os.Mkdir(CR.channelPath, CR.DefaultFileMode )


    go CR.EntryWriter( CR.entryInbox )
}




/*
func (CR *CommunityRepo) GetChannel( inChannel string ) *ChannelStore {

    // Standardize case of incoming channel?
    stdChName = strings.ToLower( inChannel )

    chStore := CR.channelStores[stdChName]
    if chStore == nil {
        chStore = new(ChannelStore)
        chStore.entry_db_filename = CR.channelPath + stdChName 


        CR.channelStores[inChannel] = chStore
    }
}


type PError int

const (
	noErr = 0

	// The signature on the entry did not match the signature computed for the given entry body and the author's corresponding verify sig
	InvalidEntrySignature = 5000

	// The given author was not found in the given access control list.
	AuthorNotFound = 5001

	// The given access control reference was not found
	AccessControlRefNotFound = 5002

	// The given entry's author does not have write permission to the associated channel
	AuthorLacksWritePermissio = 5003

	// The the entry's encrypted data failed to decode using the author's verification key.
	AuthorAuthFailed = 5004

	// The given timestamp is either excessively in the future or past
	BadTimestampErr = 5005
)




*/

// Every Channel keeps the following files:
//    a) channel data tome (unstructured binary file)
//        - the data body of incoming channel entries is first appended to this file
//        - read+append access to this file is used to optimize performance and caching (since data is never altered, only appended)
//    b) channel entry index (sqlite db)
//        - for each channel entry: timestamp_secs (int64, ascending-indexed),  entry hashname (TINYTEXT), tome_file_pos, tome_entry_len
//    c) list of the access control list history and when each one went into effect -- so it knows what to reject
//        - when a user is "added" to the channel, the owner must grant master key acces for each rev of the control list into the past (or as far as the owner wants to go)


var addr = "127.0.0.1:6380"

func main() {

	var myCR CommunityRepo
	{
        myCR.communityName = "PLAN Foundation"
        addr, _ := hex.DecodeString( "00112233445566778899aabbccddeeff44332211" )
        copy( myCR.communityAddr[:], addr )

        myCR.BootstrapCDS()
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
