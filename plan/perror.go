
package plan


import (
    "fmt"
    "strings"
)





// Perror is PLAN's commonly used error type
// https://github.com/golang/go/wiki/Errors
type Perror struct {
	Code int32
    Msg  string
    Err  error
}

// Error create a new PError
func Error(inErr error, inCode int32, inMsg string) *Perror {
	return &Perror{
		inCode,
        inMsg,
        inErr,
	}
}

// Errorf is a convenience function of Error() that uses a string formatter.
func Errorf(inErr error, inCode int32, inFormat string, inArgs ...interface{}) *Perror {
	return &Perror{
		inCode,
        fmt.Sprintf(inFormat, inArgs),
        inErr,
	}
}

func (e *Perror) Error() string {
    if e == nil {
        return "<nil>"
    }

    var s []string

    // e.Msg
    if len(e.Msg) > 0 {
        s = append(s, e.Msg)
    } else {
        s = append(s, "Perror")
    }

    // e.Code
    s = append(s, fmt.Sprintf(" {code:%d", e.Code)) 

    // e.Err
    if e.Err != nil {
        s = append(s, ", err:{")
        s = append(s, e.Err.Error())
        s = append(s, "}")
    }

    s = append(s, "}")
    
    return strings.Join(s, "")

}





const (


    // PDIEntryErrorFamily errors generally relate to pnode
    PDIEntryErrorFamily = 5000 + iota

    // BadPDIEntryFormat means the PDI entry being processed is corrupted or was created using an unsupported format
    BadPDIEntryFormat

    // CommunityNotFound means the specified community name or ID did not match any of the registered communities
    CommunityNotFound

    // ChannelNotFound means the given ChannelID was not found in the community repo
    ChannelNotFound

    // FailedToLoadChannelFromDisk means a channel failed to load all its files from its host community repo
    FailedToLoadChannelFromDisk

    // InvalidEntrySignature means the entry did not match the signature computed for the given entry body and the author's corresponding verify sig
    InvalidEntrySignature

    // AuthorNotFound means the given author was not found in the given access control list.
    AuthorNotFound

    // AccessChannelNotFound means the access channel specified by a given PDI entry was not found
    AccessChannelNotFound

    // NotAnAccessChannel means the access channel specified by a given PDI entry was not actually an access channel
    NotAnAccessChannel

    // FailedToProcessPDIHeader means decryption or unmarshalling of a PDI failed
    FailedToProcessPDIHeader
    
    // AuthorLacksWritePermission means the given PDI entry's author does not have write permission to the specified channel
    AuthorLacksWritePermission

    // BadTimestamp means a timestamp is in the excessively distant past or future
    BadTimestamp




    // SecurityErrorFamily errors relate to PLAN's Secure Key Interface (SKI)
    SecurityErrorFamily = 5100 + iota

    // CommunityKeyNotFound means a key source did not contain the requested CommunityKey for the given CommunityKeyID
    CommunityKeyNotFound

    // SigningKeyNotFound means a key source did not contain the requested private key for the given paired public key
    SigningKeyNotFound

    // EncryptKeyNotFound means a key source did not contain the requested private key for the given paired public key
    EncryptKeyNotFound

    // FailedToDecryptAccessGrant means a PDI entry body content failed to decrypt
    FailedToDecryptAccessGrant

    // FailedToProcessAccessGrant means that an error occurred while processing a PDI security access grant
    FailedToProcessAccessGrant

    // FailedToMarshalAccessGrant means an error occurred while encoding a PDI security access grant
    FailedToMarshalAccessGrant

    // FailedToDecryptCommunityData means the 
    FailedToDecryptCommunityData





)



