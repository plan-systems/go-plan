
package pnode



import (
    "database/sql"
    //"github.com/jmoiron/sqlx"

    //"fmt"
    "log"
    "os"
    "io"
    "bytes"
    //"io/ioutil"
    //"strings"
    "sync"
    "time"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    //"encoding/base64"

    //"github.com/tidwall/redcon"

    "github.com/plan-tools/go-plan/plan"
    "github.com/plan-tools/go-plan/pdi"

    // This inits in with sql, so no named import is needed
    //_ "github.com/mattn/go-sqlite3"

)



/*
// ValidateCitedAccessChannel checks inHeader.AccessChannel is valid to cite for the given channel store.
func (CS *ChannelStore) ValidateCitedAccessChannel( inHeader *plan.PDIEntryHeader ) error {

    var err error

    {
        expireTime := plan.DistantFuture
        var acaMatch *AccessChannelAssignment

        acHistory := &CS.ChannelAdmin.AccessChannelHistory
        for i := len(*acHistory) - 1; i >= 0; i-- {
            aca := &(*acHistory)[i]
            if aca.AccessChannelID == inHeader.AccessChannelID {
                if aca.InEffect <= inHeader.Time && inHeader.Time < expireTime {
                    acaMatch = aca
                    break
                } else {
                    err = plan.Errorf( ExpiredAccessChannel, "The entry cited access channel 0x%x is expired", inHeader.AccessChannelID )  
                }
            }
            expireTime = aca.InEffect
        }

        if acaMatch == nil {
            if err == nil {
                err = plan.Errorf( InvalidAccessChannel, "The access channel 0x%x was not found for channel 0x%x", inHeader.AccessChannelID, inHeader.ChannelID )
            }
            return err       
        }
    }

    return err
}
*/




const (
    ChannelIndexFilename        = "idx.db"
    ChannelTomeFilename         = "01"
    //ChannelAdminFilename        = "ChannelAdmin.db.json"
    ChannelPropertiesFilename   = "ChannelProperties.json"
    PnodeConfigFilename         = "config.json"

    tomeNumShift                = byte( 64 - 16 )
    tomeNumMask                 = 0xFFFF << tomeNumShift
    tomeOffsetMask              = ( 1 << tomeNumShift ) - 1
)


type ChannelActionVerb string
const (

    // VerbChannelGenesis creates a new channel, specifying a complete set of channel properties via an encoded form of ChannelProperties
    VerbChannelGenesis              = "genesis"

    // VerbChannelSetProperties sets a specified number of channel properties.  .VerbData is a json encoded list of param keys and new values.
    VerbChannelSetProperties        = "set"

)


// ChannelStore manages a memory-resident representation of any PLAN channel
type ChannelStore struct {
    sync.RWMutex

    ChannelID               plan.ChannelID

    // PrevAccessTime specifies when this channel store was last accessed, used to know how long a channel store has been idle.
    PrevActivityTime        plan.Time
    
    // AccessRank specifies what index this channel store is in its parent ChannelStoreGroup
    //AccessRank              int32 

    ChannelInfo             pdi.ChannelInfo

    channelDir              string

    // ChannelEpochs is list of all epochs for this channel since its creation (sorted with newer epochs appearing first) 
    ChannelEpochs           []*pdi.ChannelEpoch

    db                      *sql.DB
    select_timeMatch        *sql.Stmt
    select_hashMatch        *sql.Stmt
    insert_entry            *sql.Stmt

    tome_file               *os.File
    
    // ACStore is ONLY set if this channel is being used as an AccessChannel.
    ACStore                 *ACStore
}




// ACStore represents an permissions/access control container.  Entries to a AccessChannelID appear as
//     a given public 
type ACStore struct {

    // This represents the db that contains a sequential list of 
    //GrantsHistory           []pdi.AccessGrantBatch

    // This is a composite of all entries posted to this access channel.  This table is built up by compositing
    //    all access grant history posted to this access channell, going in order of time. 
    AccessByAuthor          map[plan.IdentityAddr]pdi.ChannelAccess

}






// ChannelAdminAction is a record of a change of one or more channel properties *or* an action associated with the channel (e.g. channel rekey event) 
// During a channel's lifetime, it be set so that a different access channel governs permissions for this channel.
type ChannelAdminAction struct {

   // The time of this channel admin action
   Time                    plan.Time                    `json:"when"`

   // Who authored this action (only channel owners are allowed to insert channel admin actions)
   Author                  plan.IdentityAddr            `json:"author"`

   // ChannelAction is a json encoding of a sequence of channel params and actions that are set/executed.
   ChannelAction           []byte                       `json:"action"`
 
}

type AccessChannelAssignment struct {
 
   // The time of this channel admin action
   InEffect                plan.Time                   `json:"when"`

   AccessChannelID         plan.ChannelID              `json:"acID"`

   AccessChannelRev        int                         `json:"acRev"`
}

/*
// ChannelAdmin is a time-sequential list of channel meta chanages
type ChannelAdmin struct {

   AccessChannelHistory    []AccessChannelAssignment   `json:"acHistory"`

   ActionHistory           []ChannelAdminAction        `json:"actionLog"`

}

*/


func (CS *ChannelStore) IsAccessControlChannel() *ACStore {

    return CS.ACStore
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


    return TomePosEncoding( ( inTomeNum << byte(tomeNumShift) ) | pos ), nil
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

        // TODO: Use file pool that auto-flushes and closes files after X seconds or as file access goes past N txns, etc.
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
            flags               INT,            /* Reserved for when entries are "disabled", etc                */
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




// WriteEntryToStorage inserts the given entries to the this channel
// Pre: CS.channelName == inEntries[:].data.channelName
func (CS *ChannelStore) WriteEntryToStorage( inEntry *pdi.EntryCrypt, inHdr *pdi.EntryHeader ) error {

    plan.Assert( bytes.Equal(inHdr.ChannelId, CS.ChannelID[:]), "Bad channel given to InsertEntriesToChannel" )


    err := CS.OpenIndexDB()
    if err != nil {
        return err
    }

    {
        var err error
        var matchRow int 

        {
            err = CS.select_timeMatch.QueryRow( inHdr.TimeAuthored ).Scan( &matchRow )
            if err == sql.ErrNoRows {

            } else {
                log.Printf( "got possible dupe!" )

                //err = CS.select_timeMatch.QueryRow( matchRow, *inEntry.Hash ).Scan( &matchRow )

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



*/




// Every Channel keeps the following files:
//    a) channel data tome (unstructured binary file)
//        - the data body of incoming channel entries is first appended to this file
//        - read+append access to this file is used to optimize performance and caching (since data is never altered, only appended)
//    b) channel entry index (sqlite db)
//        - for each channel entry: timestamp_secs (int64, ascending-indexed),  entry hashname (TINYTEXT), tome_file_pos, tome_entry_len
//    c) list of the access control list history and when each one went into effect -- so it knows what to reject
//        - when a user is "added" to the channel, the owner must grant master key acces for each rev of the control list into the past (or as far as the owner wants to go)


