

package plan

import (
    "fmt"
)


const (
    AddrByteLen         = 20
    
    PDIEntrySigSz       = 30
    PDIEntryHashSz      = 30
    AccessCtrlAddrSz    = 10
    ChannelAddrSz       = 20
)

type CommunityAddr          [AddrByteLen]byte
type IdentityAddr           [AddrByteLen]byte
type PDIEntrySig            [PDIEntrySigSz]byte
type PDIEntryHash           [PDIEntryHashSz]byte
type ChannelId              [ChannelAddrSz]byte

/*
For a given community, CommunityKeyId identifies a specific shared "community-global" symmetric key.  
When a PLAN client starts a session with a pnode, the client sends the pnode her community-public keys.  
PDIEntryCrypt.CommunityKeyId specifies which community key was used to encrypt PDIEntryCrypt.Header. 
If/when an admin of a community issues a new community key,  each member is securely sent this new key via
the community key channel (where is key is asymmetrically sent to each member still "in" the community. 
*/
type AccessCtrlId           [AccessCtrlAddrSz]byte


const (
    
    // PDIEntry param keys (and values where appropriate)
    PDI_cmd                         = "cmd"
        PDI_cmd_PostEntry           = "post"
        PDI_cmd_ReplaceEntry        = "replace"
        PDI_cmd_RevokeEntry         = "remove"
    PDI_TimeAuthored                = "tsAuthor"
    
    // PDIEntry param keys exported to client (keys not in PDIEntryBody.params since they reside in PDIEntry and would be a waste to duplicate)
    PDI_ChannelId                   = "ch"   
    PDI_TimeReceived                = "tsRecv"
    PDI_TimeConsensus               = "tsConsensus"
    PDI_AccessCtrlId                = "ac"
    PDI_Author                      = "author"
)


 // Contains flags and info about this entry, in effect specifying which hash functions and crypto to apply.
 type PDIEntryInfo          uint32
 type PDIEntryVers          byte
 const (
    PDIEntryVers_1          byte = 1 + iota

    PDIEntryVersMask        uint32 = 0xFF
 )



 // Unix time (UTC in seconds elapsed since Jan 1, 1970)
 type Timestamp             uint64

/*
 type PDIEntrySchema struct {
    Hasher                  hash.Hash
    Signer                  interface{}
 } 



 var PDIEntrySchemas = [2]PDIEntrySchema { 
    PDIEntrySchema{
    },
    PDIEntrySchema { 
        Hasher:     hash.SHA256,
        Signer:     nil,
    },
}

 
*/



type PDIEntryHeader struct {
    Nonce                   uint64              // Prevents replay attacks -- should be incremented by the client each entry sealed.
    Timestamp               uint64              // Unix time in seconds UTC when this entry was published
    Cmd                     string              // PDI command/verb
    ChannelId               ChannelId           // The channel id this entry is posted to.
	Author                  IdentityAddr        // Creator of this entry (and signer of .Sig)
	AccessCtrlId            AccessCtrlId        // Identifies the permissions channel (an access control implemenation) this entry was encrypted with 
    Params                  map[string]string   // Additional param list (PDI-level key-value params) -- PDI_param_*: <value>
}

// Every PDI Entry has one or more data parts.  Each part has its own multistream-codec style description header ("/protocol/sub-protocol")
type PDIBodyPart struct {
	Header                  string              // <multistream-codec-header> -- Human-readable, UTF8 string that describes .body (e.g. "/json/rpc")
	Body                    []byte              // Arbitrary client binary data conforming to .header
}


// PDIEntry.dataCrypt decrypts and deserializes into a PDIEntryBody instance and is an abstract payload container. 
type PDIEntryBody struct {
    Nonce                   uint32              // Prevents replay attacks -- should be incremented by the client each entry sealed.
	BodyParts               []PDIBodyPart       // Zero or more data sections -- e.g. .parts[0] is a serialized json param list and .parts[1] is a blob of rich text (rtf).
}


/*
PDIEntry (transmission medium to/from communuity data store implementations.)

A PDI entry has two parts, the header and body segment.  The PDIEntryCrypt.Header is encrypted using a community-public key, 
specified by PDIEntryCrypt.CommunityKeyId.  This ensures non-community members (i.e. public snoops) can't see any entry params,
data, or even to what channel id the entry is being post to.  PDIEntryCrypt.Body is encrypted by the specified community key *or* 
by the key specified via PDIEntryHeader.Author and PDIEntry.AccessCtrlId (all via the client's Secure Key Interface).  In sum,
PDIEntry.AccessCtrlId specifies a decryption flow through the user's private, compartmentalized keychain.

When a PLAN client starts a new session with a pnode, the client sends the community keys for the session.  This allows
pnode to process and decrypt incoming PDIEntryCrypt entries from the storage medium (e.g. Ethereum).  Otherwise, pnode
has no ability to decrypt PDIEntryCrypt.Header.  A pnode could be configured to keep the communty keychain even when there
are no open client PLAN sessions so that incomning entries can be processed.  Otherwise, incoming entries won't be processed
from the lowest level PDI storage layer.  Both configurations are reasonable depending on security preferences.

When a community admin initiates a community-key "rekey event", the newly generated community key is securely and individually
"sent" to each community member via the community's public key transfer channel. The new community key is encrypted using each member's public key.  
When a pnode is processing an entry that it does not have a community key for, it will check the community's public key channel
for an entry for the current client's public key, it will send the client the encrypted community key.  The client uses its SKI 
to decrypt the payload into the new community key.  This key is added to the user's SKI keychain and is sent back to pnode.

When pnode sends a PLAN client is PDI entries, it sends PDIEntry.Header (via json) and PDIEntryCrypt.Body.  If the 

Recall that the pnode client has no abilty to decrypt PDIEntryCrypt.Body if PDIEntryHeader.AccessCtrlId isn't set for
community-public permissions.  This is fine since only PLAN clients with a 

And awaaaaay we go!
*/

type PDIEntryCrypt struct {
	Sig                     PDIEntrySig         // Signature of PDIEntry.Hash (signed by PDIEntry.Header.Author)
    Hash                    *PDIEntryHash       // If set, hash of ALL other fields in this entry (except .Sig)

    Info                    PDIEntryInfo        // Entry type info, Allows PDIEntry accessors to apply the correct hash and crypto functions
    CommunityKeyId          AccessCtrlId

    Header                  *[]byte             // Encrypted using a community key, referenced via .CommunityKeyId
    Body                    *[]byte             // Encrypted using a community key or a user private key (based on PDIEntry.Header)

}

type PDIEntry struct {
    Crypt                  *PDIEntryCrypt       // Originating (encrypted) entry data used to instantiate this data

    HeaderBuf               *[]byte             // Serialized representation of PDIEntry.Header  (decrypted form PDIEntryCrypt.Header)
    Header                  PDIEntryHeader

    BodyBuf                 *[]byte             // Serialized representation of PDIEntry.Body (decrypted from PDIEntryCrypt.Body)
	Body                    *PDIEntryBody



 

    // When a new PDIEntry arrives off the wire, .Sig is set, .Hash == nil, .dataCrypt is set, .Body == nil
    //    1) .dataCrypt is hashed via  PDIEntrySchema.BodyHasher
    //    2) .Sig is verfified based on the hash calculated from (1)
    //    3) Use .AccessCtrlId to specifiy a decryption flow through PLAN's Secure Key Interface (SKI).
    //         - If the local user pubkey does not have "read" access, then that means there's a db corresponding to that access list that contains a list of all entries
    //           that have been encountered (and can't be unlocked).  In this case, referencing info for this entry is appended to this db and entry is not
    //           processed further.  If/when the CommunityRepo sees an access control list get unlocked for the current logged in user, it will 
    //         - 
    // , then the entry data can only be stored until the user is granted access.
    //    2) Compute Hash of 
    //    2) Decrypt .dataCrypt into .dataRLP (a generic blob) 
    //    3) Deserialize this output blob (instantiate .Body).  

}


/*
type CommunutyKeyGateway interface {

    ComputeHash() *PDIEntryHash

}
*/
/*
func (inEntry PDIEntry*) ComputeHash() *PDIEntryHash {
    if ( inEntry.Info & PDIEntryVersMask ) == PDIEntryVers_1 {
        
    }
    return nil
}

*/




func Assert( inCond bool, inFormat string, inArgs ...interface{} ) {

    if ! inCond {
        panic( fmt.Sprintf( inFormat, inArgs ) ) 
    }
}



/*
Opening a private message channel flow where Alice wants to talk to Bob:
1) Alice uses UI to open a "private message channel" to Bob
2) PLAN makes a new channel named "/talk/<Bob>/<title>" and it is encrypted based on the new ACL and published)
4a)  Some time later, Bob's client gets the new entries from the network
4bc) Bob's pnode sees a new channel appear having an ACL that he has access to
4bc) Bob's pnode sees a an ACL appear that his pubkey has access to
5) Bob's pnode is able to process and decrypt new posts made to this new channel
6) Bob's PLAN client gets the new posts to master-community ACL and master channel registry
7) Bob's client notifies Bob that he has been given access to a new channel.  
*/