

package main

import (
	"database/sql"
    //"fmt"
    "flag"
	"log"
    "os"
    "io"
    "io/ioutil"
	"strings"
    "sync"
    "time"
    //"sort"
    "encoding/hex"
    "encoding/json"
    "encoding/base64"

    //"github.com/tidwall/redcon"

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
    "crypto/rand"

    //"github.com/stretchr/testify/assert"

	"github.com/ethereum/go-ethereum/rlp"
    "github.com/ethereum/go-ethereum/common/hexutil"

)

/*
const (
    DEBUG     = true
)
*/





const (
	noErr = 0

	// The signature on the entry did not match the signature computed for the given entry body and the author's corresponding verify sig
	InvalidEntrySignature = 5000 + iota

	// The given author was not found in the given access control list.
	AuthorNotFound = 5001

	// The given access control reference was not found
	AccessChannelNotFound = 5002

    InvalidAccessChannel

    ExpiredAccessChannel 

    FailedToCreateChannelOnDisk

    NotAnAccessChannel

    ChannelAlreadyExistsErr

	// The given entry's author does not have write permission to the associated channel
	AuthorLacksWritePermission = 5003

	// The the entry's encrypted data failed to decode using the author's verification key.
	AuthorAuthFailed = 5004

	// The given timestamp is either excessively in the future or past
    BadTimestampErr = 5005
    

)




// ServerSession is an interface available to clients to call against
type PnodeAPI interface {

    // PublishEntry accepts a pdi entry from a live client and attempts to post it to this community repo
    PublishEntry( inEntry *plan.PDIEntryCrypt )
}



type ClientInfo struct {

    UserID                  plan.IdentityAddr        // Creator of this entry (and signer of .Sig)    
}

type ClientSession interface {
    ClientInfo() *ClientInfo

    OnLogin( inAPI PnodeAPI )
    OnLogout()

    // Encrypts/Decrypts the given data, given a community key id.
    DecryptCommunityData( inCommunityKey plan.CommunityKeyID, inEncrypted []byte ) ( []byte, error )
    EncryptCommunityData( inCommunityKey plan.CommunityKeyID, inData      []byte ) ( []byte, error )
}



type ChannelStoreLookup struct {
    sync.RWMutex

	table                   map[plan.ChannelID]*ChannelStore
}




type CommunutyRepoInfo struct {
	CommunityName           string                  `json:"communityName"`
    CommunityID             hexutil.Bytes           `json:"communityID"`
    RepoPath                string                  `json:"repoPath"`
    ChannelPath             string                  `json:"channelPath"`
}


// Community Data Repository
type CommunityRepo struct {
    Info                    *CommunutyRepoInfo

    // This will move and is just a hack for now
    activeSession           ClientSession

    DefaultFileMode         os.FileMode
    DirEncoding             *base64.Encoding

    // Incoming encrypted entries "off the wire" that are ready to be processed (and written to local community repo)
    entriesToProcess        chan *plan.PDIEntryCrypt


    channelStores           ChannelStoreLookup      // Turns a plan.ChannelID into a *ChannelStore
}





func (CR *CommunityRepo) InitChannelStore( CS *ChannelStore, inCreateNew bool ) error {

    CS.Lock()

    CR.channelStores.Lock()
    CR.channelStores.table[CS.Properties.ChannelID] = CS
    CR.channelStores.Unlock()

    CS.channelDir = CR.Info.ChannelPath + CR.DirEncoding.EncodeToString( CS.Properties.ChannelID[:] ) + "/"

    if inCreateNew {

        err := os.Mkdir( CS.channelDir, CR.DefaultFileMode )
        if err != nil {
            return plan.Errorf( FailedToCreateChannelOnDisk, "Failed to create channel dir on disk %T", err )
        }

        buf, err := json.Marshal( &CS.ChannelAdmin )
        if err == nil {
            err = ioutil.WriteFile( CS.channelDir + ChannelAdminFilename, buf, CR.DefaultFileMode )
        }
    
        buf, err = json.Marshal( &CS.Properties )
        if err == nil {
            err = ioutil.WriteFile( CS.channelDir + ChannelPropertiesFilename, buf, CR.DefaultFileMode )
        }

    } else {

        buf, err := ioutil.ReadFile( CS.channelDir + ChannelAdminFilename )
        if err == nil {
            err = json.Unmarshal( buf, &CS.ChannelAdmin )
        }

        buf, err = ioutil.ReadFile( CS.channelDir + ChannelPropertiesFilename )
        if err == nil {
            err = json.Unmarshal( buf, &CS.Properties )
        }

    }


    if CS.Properties.IsAccessChannel {

        CS.ACStore = new( ACStore )

        if inCreateNew {

        } else {

        }
    }

    return nil
}






func (CR *CommunityRepo) PublishEntry( inEntry *plan.PDIEntryCrypt ) {

    CR.entriesToProcess <- inEntry
}



func (CR *CommunityRepo) EntryProcessor() {


    for entry := range CR.entriesToProcess {

        CR.processEntry( entry )
    }
}





func (CR *CommunityRepo) processEntry( ioEntry *plan.PDIEntryCrypt ) error {

    entry := new( plan.PDIEntry )
    entry.PDIEntryCrypt = ioEntry

    var err error

    // Community data is keyed using one of the community keys.
    entry.HeaderBuf, err = CR.DecryptCommunityData( ioEntry.CommunityKeyID, ioEntry.HeaderCrypt )
    if err != nil {
        return err
    }

    // Deserialize inEntry.Headerbuf into inEntry.Header
    entry.Header = new( plan.PDIEntryHeader )
    err = rlp.DecodeBytes( entry.HeaderBuf, entry.Header )
    if err != nil {
        return err
    }

    // Used in various places
    ioEntry.Hash = new( plan.PDIEntryHash )
    ioEntry.ComputeHash( ioEntry.Hash )

    // Now that we've decrypted and deserialized the header, we can verify the entry's signature
    err = CR.VerifySig( ioEntry )
    if err != nil {
        return err
    }

    // Fetch (or load and fetch) the ChannelStore assicated with the given channel
    CS, err := CR.GetChannelStore( &entry.Header.ChannelID, PostingToChannel | LoadIfNeeded )

    verb := entry.Header.Verb
    switch ( verb ) {

        case plan.PDIEntryVerbPostEntry:

            // First, we must validate the access channel cited by the header used by the author to back permissions for posting this entry.
            // This checks that the author didn't use an invalid or expired access channel to post this entry.  Once we validate this, 
            //    we can trust and use that access channel to check permissons further.
            err = CS.ValidateCitedAccessChannel( entry.Header );

            err = CR.VerifyWriteAccess( CS, entry.Header )
            if err != nil {
                return err
            }

            err := CS.WriteEntryToStorage( entry )
            if err != nil {
                return err
            }

        case plan.PDIEntryVerbChannelAdmin:

            // In general, if the channel already exists, it's an error.  Howeever we need to check if this entry 
            if CS != nil {
                //err = plan.Error( )
            }

        default:
            plan.Assert( false, "Unhandled verb" )

    } 



    return err

}



const (
    AuthorHasReadAccess        = 0x01
    AuthorHasWriteAccess       = 0x02
    AuthorHasOwnAccess         = 0x04
)

type AuthorAccessFlags  uint32



/*
There are 6 user channel access states:

I) Channel community-public or private?
    (a) community-public -- entry body encrypted with a community (symmetric) key and the listed AccessChannelID specifies
                            what default permissions are and what users are excluded from that (e.g. default read-only but
                            users Alice and Bob have write access, while only Bob has own access)
    (b) private          -- entry body encrypted with the channel key "sent" to users via AccessChannelID.  Users are granted the key for
                            read access, and potentially an additional "write" or "own" flag (that pnodes verify/enforce)

II) AccessChannelID privilege levels:
    (1) read-only        -- User have symettric key access but pnodes reject posts if the author only had read permissions 
    (2) read + write     -- Users have additional post flag set (so pnodes see a given author is permitted to post a new entry)
    (3) owner            -- Can read, write, and grant others users read and write access privs.


*/

type GetChannelStoreFlags int32
const (
    ReadingFromChannel          = 0x01
    PostingToChannel            = 0x02

    IsAccessChannel             = 0x10

    LoadIfNeeded                = 0x30
)

func (CR *CommunityRepo) GetChannelStore( inChannelID *plan.ChannelID, inFlags GetChannelStoreFlags ) ( *ChannelStore, error ) {

    CR.channelStores.RLock()
    CS := CR.channelStores.table[*inChannelID]
    CR.channelStores.RUnlock()

    var err error
    var hasWriteLock bool

    if CS == nil && ( inFlags & LoadIfNeeded ) != 0 {
        CS = new( ChannelStore )
        CS.Properties.ChannelID = *inChannelID

        err = CR.InitChannelStore( CS, false )
        if err != nil {
            CR.channelStores.Lock()
            delete( CR.channelStores.table, *inChannelID )
            CR.channelStores.Unlock()
            CS = nil

            // TODO: keep log of failed CS that are fail to be loaded
        } else {
            hasWriteLock = true
        }
    }

    if err == nil {
        if ( inFlags & PostingToChannel ) != 0 {
            if ! hasWriteLock {
                CS.Lock()
            }
        } else if ( inFlags & ReadingFromChannel ) != 0 {
            if hasWriteLock {
                CS.Unlock()
            }
            CS.RLock()
        } 
    }


    return CS, err
}




// VerifyAccess checks that the given PDI Entry has the proper permissions to do what it says it wants to do and that
//    the AccessChannelID cited is in fact a valid access channel to cite (given the timestamp of the entry, etc)
func (CR *CommunityRepo) VerifyWriteAccess( CS *ChannelStore, inHeader *plan.PDIEntryHeader ) error {

    // Get/Load/Create the data structure container for the cited access channel
    AC, _ := CR.GetChannelStore( &inHeader.AccessChannelID, IsAccessChannel | ReadingFromChannel | LoadIfNeeded )
    if AC == nil {
        return plan.Errorf( AccessChannelNotFound, "cited access channel 0x%x not found", inHeader.AccessChannelID )
    }
    if AC.ACStore == nil {
        return plan.Errorf( NotAnAccessChannel, "cited access channel 0x%x not actually an access channel", inHeader.AccessChannelID )
    }

    // Entries posted to a channel cite (and use) the latest/current AccessChannelID assiciated with the channel.
    // ...but pnodes must check this!


    {
        access := AC.ACStore.AccessByAuthor[inHeader.Author]
        if ( access & AuthorHasWriteAccess ) == 0 {
            return plan.Error( AuthorLacksWritePermission, "Author does not have write access to channel" )
        }
    }

    return nil

}


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







func (CR *CommunityRepo) DecryptCommunityData( inKeyID plan.CommunityKeyID, inEncrypted []byte ) ( []byte, error ) {

    return CR.activeSession.DecryptCommunityData( inKeyID, inEncrypted )

}

func (CR *CommunityRepo) EncryptCommunityData( inKeyID plan.CommunityKeyID, inData []byte ) ( []byte, error ) {

    return CR.activeSession.EncryptCommunityData( inKeyID, inData )
    
}



// inSig is the sig data block
// inHash specifies how inHashed was created (or 0 if not inHashed is not hashed)
// inHashed is the hash digest that is said to be signed by inSigner into inSig
// This function verifies that inSig a signature of inHashed from the private key associated with inSigner

func (CR *CommunityRepo) VerifySig( inEntry *plan.PDIEntryCrypt ) error {

    //var err error

    
    // For next step, see:
    // https://github.com/ethereum/go-ethereum/blob/master/crypto/crypto_test.go
    // https://github.com/ethereum/go-ethereum/tree/master/crypto

    return nil
    
}


/*
func (CR *CommunityRepo) HashAndSign( ioEntry *plan.PDIEntry ) error {

    entryHash := inEntry.ComputeHash()

    // See above

    return nil
    
}

*/



const (
    ChannelIndexFilename        = "idx.db"
    ChannelTomeFilename         = "01"
    ChannelAdminFilename        = "ChannelAdmin.db.json"
    ChannelPropertiesFilename   = "ChannelPropertiesFilename.json"
    PnodeConfigFilename         = "config.json"

    kTomeNumShift               = byte( 64 - 16 )
    kTomeNumMask                = 0xFFFF << kTomeNumShift
    kTomeOffsetMask             = ( 1 << kTomeNumShift ) - 1
)


type ChannelActionVerb string
const (

    // VerbChannelGeneis creates a new channel, specifying a complete set of channel properties via an encoded form of ChannelProperties
    VerbChannelGeneis               = "genesis"

    // VerbChannelSetProperties sets a specified number of channel properties.  .VerbData is a json encoded list of param keys and new values.
    VerbChannelSetProperties        = "set"

)


// ChannelStore manages a memory-resident representation of any PLAN channel
type ChannelStore struct {
    sync.RWMutex

    Properties              plan.ChannelProperties

    channelDir              string

    // ChannelActionLog is an ordered list of ChannelAction executed on this channel, starting from when the channel was first created,
    //    all the way up to the present moment.  This log allows a pnode to verify that a given post to a channel is citing
    //    a valid, available, and legal access channel (given an author and timestamp)
	ChannelAdmin             ChannelAdmin

    db                      *sql.DB
    select_timeMatch        *sql.Stmt
    select_hashMatch        *sql.Stmt
    insert_entry            *sql.Stmt

    tome_file               *os.File
    
    // ACStore is ONLY set if this channel is being used as an AccessChannel.
    ACStore                 *ACStore
}


type AccessGrant struct {
    Timestamp               plan.Time

    Granter                 plan.IdentityAddr
    Grantee                 plan.IdentityAddr

    // When a channel owner grants access to someone, she is really using the grantee's public key to encrypt 
    //    the AccessCtrl's master/private key and posting it to the access control list.  Keep in mind that pnodes
    //    validate incoming grants by ensuring that .Granter has ownership-level permissions.
    // Since some permissions changes don't require the key to be resent, this value is sometimes nil.
    EncryptedChannelKey     *plan.EncryptedChannelKey

    // 
    UpdatedAccess           AuthorAccessFlags
}

// ACStore reperesents an permissions/access control container.  Entries to a AccessChannelID appear as
//     a given public 
type ACStore struct {

    // This represetns the db that contains a sequentual list of 
    RunningHistory          []AccessGrant

    // This is a composite of all entries posted to this access channel.  This table is built up by compositing
    //    all access grant history posted to this access channell, going in order of time. 
    AccessByAuthor          map[plan.IdentityAddr]AuthorAccessFlags

}





const (

    // PDIEntryVerbCreateChannel creates a new channel (not common)
   ChannelAdminSetAccessChannel       = "set"

)

// ChannelAdminAction is a record of a change of one or more channel peoperties *or* an action associated with the channel (e.g. channel rekey event) 
// During a channel's lifetime, it be set so that a different access channel governs permissions for this channel.
type ChannelAdminAction struct {

   // The time of this channel admin action
   Time                    plan.Time                    `json:"when"`

   // Who authored this action (only channel owners are allowd to insert channel admin actions)
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

// ChannelAdmin is a time-seqential list of channel meta chanages
type ChannelAdmin struct {

   AccessChannelHistory    []AccessChannelAssignment   `json:"acHistory"`

   ActionHistory           []ChannelAdminAction        `json:"actionLog"`

}




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






// Inserts the given entries to the this channel
// Pre: CS.channelName == inEntries[:].data.channelName
func (CS *ChannelStore) WriteEntryToStorage( inEntry *plan.PDIEntry ) error {

    plan.Assert( inEntry.Header.ChannelID == CS.Properties.ChannelID, "Bad channel given to InsertEntriesToChannel" )


    err := CS.OpenIndexDB()
    if err != nil {
        return err
    }

    {
        var err error
        var matchRow int 

        {
            err = CS.select_timeMatch.QueryRow( inEntry.Header.Time ).Scan( &matchRow )
            if err == sql.ErrNoRows {

            } else {
                log.Printf( "got possible dupe!" )

                err = CS.select_timeMatch.QueryRow( matchRow, *inEntry.PDIEntryCrypt.Hash ).Scan( &matchRow )

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






type PNode struct {
    CRbyID                      map[plan.CommunityID]*CommunityRepo

    config                      PNodeConfig

    activeSessions              []ClientSession

    BasePath                    string

}



type PNodeConfig struct {
    Name                        string                          `json:"nodeName"`
    PNodeID                     hexutil.Bytes                   `json:"nodeID"`

    RepoList                    []CommunutyRepoInfo             `json:"repoList"`

    DefaultFileMode             os.FileMode                     `json:"defaultFileMode"`
}







func init() {



}



func (pn *PNode) AllocCommunityRepo( inInfo *CommunutyRepoInfo ) *CommunityRepo {

    CR := new( CommunityRepo )
    CR.Info = inInfo
    CR.DirEncoding = base64.RawURLEncoding

    // Runtime support
    CR.entriesToProcess = make( chan *plan.PDIEntryCrypt,    32 )
    CR.DefaultFileMode = pn.config.DefaultFileMode

    return CR
}


/*
// PNodeID identifies a particular pnode running as part of a PLAN community
type PNodeID                [32]byte

func (pnID PNodeID) MarshalJSON() ( []byte, error ) {
    bytesNeeded := base64.RawURLEncoding.EncodedLen( len(pnID) ) + 2
    outText := make( []byte, bytesNeeded, bytesNeeded )
    outText[0] = '"'
    base64.RawURLEncoding.Encode( outText[1:bytesNeeded-1], pnID[:] )
    outText[bytesNeeded-1] = '"'

    return outText, nil
}

func (pnID PNodeID) UnmarshalJSON( inText []byte ) error {
    _, err := base64.RawURLEncoding.Decode( pnID[:], inText )

    return err
}
*/

func (pn *PNode) InitOnDisk() {

    pn.config.PNodeID = make( []byte, plan.IdentityAddrSz )
    rand.Read( pn.config.PNodeID )

    pn.WriteConfigOut()
}


func (pn *PNode) LoadConfigIn() error {


    buf, err := ioutil.ReadFile( pn.BasePath + PnodeConfigFilename )
    if err == nil {
        err = json.Unmarshal( buf, &pn.config )
    }

    for i := range pn.config.RepoList {
        CRInfo := &pn.config.RepoList[i]
        
        CR := pn.AllocCommunityRepo( CRInfo )

        pn.RegisterCommunityRepo( CR )
    }

    return err
}

func (pn *PNode) RegisterCommunityRepo( CR *CommunityRepo ) {

    var communityID plan.CommunityID
    copy( communityID[:], CR.Info.CommunityID )

    pn.CRbyID[communityID] = CR

}

func (pn *PNode) WriteConfigOut() error {

    os.MkdirAll( pn.BasePath, pn.config.DefaultFileMode )

    buf, err := json.MarshalIndent( &pn.config, "", "\t" )
    if err != nil {
        return err
    }

    err = ioutil.WriteFile( pn.BasePath + PnodeConfigFilename, buf, pn.config.DefaultFileMode )

    return err

}





func (pn *PNode) CreateNewCommunity( inCommunityName string ) *CommunityRepo {

    pn.config.RepoList = append( pn.config.RepoList, CommunutyRepoInfo{} )

    info := &pn.config.RepoList[len(pn.config.RepoList)-1]
    
    {
        info.CommunityName = inCommunityName
        info.CommunityID = make( []byte, plan.IdentityAddrSz )
        rand.Read( info.CommunityID )

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
    
            for _, r := range strings.ToLower( info.CommunityName ) {
                if replace, ok := remapCharset[r]; ok {
                    if replace != 0 {
                        b.WriteRune(replace)
                    }
                } else {
                    b.WriteRune(r)
                }
            }
    
    
            info.RepoPath = b.String() + "-" + hex.EncodeToString( info.CommunityID[:4] ) + "/"
            info.ChannelPath = info.RepoPath + "ch/"
        }

    }

    CR := pn.AllocCommunityRepo( info )

    pn.RegisterCommunityRepo( CR )

    pn.WriteConfigOut()

    return CR

}


// ONLY Uused for below call -- delete once network connects are setup
var gPNode *PNode

func PNode_StartFakeSession( inCommunityName string, inSession ClientSession ) {

    for _, CR := range gPNode.CRbyID {

        if ( CR.Info.CommunityName == inCommunityName ) {
            CR.activeSession = inSession
        
            gPNode.activeSessions = append( gPNode.activeSessions, inSession )
            inSession.OnLogin( CR )

            return
        }


    }

}


func PNode_main() {
    var pn PNode

    dataDir     := flag.String( "datadir",      "",         "Directory for config files, keystore, and communuity repos" )
    init        := flag.Bool  ( "init",         false,      "Initializes <datadir> as a fresh pnode" )

    flag.Parse()

    // Set config defaults
    pn.BasePath = *dataDir //"/Users/aomeara/_pnode/"
    pn.config.DefaultFileMode  = os.FileMode(0775)
    pn.CRbyID = make( map[plan.CommunityID]*CommunityRepo )
   
    if ( *init ) {
        pn.InitOnDisk()
    }


    err := pn.LoadConfigIn()
    if err != nil {
        panic( err )
    }

    // TEST Make new community repo
    if ( false ) {
        pn.CreateNewCommunity( "PLAN Foundation" )
    }

    // TODO: for each community repo a pnode is hosting, open up a port that serves clients.
    // Until then, each communty repo is started manually
    for _, CR := range pn.CRbyID {
        go CR.EntryProcessor()
    }

    /*
    {
        socket, err := net.Listen( "tcp", ":8080" )
        if err != nil {
            // handle error
        }
        for {
            conn, err := socket.Accept()
            if err != nil {
                // handle error
            }
            go handleConnection( conn )
        }
    }*/

    // TODO: delete this once network connections are up
    gPNode = &pn

    // Wait forever
    c := make(chan struct{})
    <-c

}