package plan

import (
	"fmt"
	"strings"
)

// Assert asserts essential assumptions
func Assert(inCond bool, inMsg string) {

	if !inCond {
		panic(inMsg)
	}
}

// Assertf asserts essential assumptions
func Assertf(inCond bool, inFormat string, inArgs ...interface{}) {

    Assert(inCond, fmt.Sprintf(inFormat, inArgs...))

}

/*****************************************************
** Err / plan.Error()
**/

// GUI error philosophy: errors can be suppressed by type or by item that they are for.

// Err is PLAN's common error struct.  Err.Code allows easy matching while allowing error strings to contain useful contextual information.
type Err struct {
	Status

	Err error
}

// Error create a new PError
func Error(inErr error, inCode int32, inMsg string) *Err {
	return &Err{
		Status{
			Code: inCode,
			Msg:  inMsg,
		},
		inErr,
	}
}

// Errorf is a convenience function of Error() that uses a string formatter.
func Errorf(inErr error, inCode int32, inFormat string, inArgs ...interface{}) *Err {
	return &Err{
		Status{
			Code: inCode,
			Msg:  fmt.Sprintf(inFormat, inArgs...),
		},
		inErr,
	}
}

// Error implements error's Error()
func (e *Err) Error() string {
	if e == nil {
		return "<nil>"
	}

	var s []string

	// e.Msg
	if len(e.Msg) > 0 {
		s = append(s, e.Msg)
	} else {
		s = append(s, "Err")
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

// IsError tests if the given error is a PLAN error code (below)
func IsError(inErr error, inErrCodes ...int32) bool {
    if inErr == nil {
        return false
    }
    if perr, ok := inErr.(*Err); ok && perr != nil {
        for _, errCode := range inErrCodes {
            if perr.Code == errCode {
                return true
            }
        }
    }

    return false
}

const (

	/*****************************************************
	** Universal errors
	**/

	// GenericErrorFamily errors generally relate to pnode
	GenericErrorFamily int32 = 5000 + iota

	// AssertFailed means an unreachable part of code was...reached.  :\
	AssertFailed

	// ParamMissing one or more params was missing, nil, or not otherwise given
	ParamMissing

	// MarshalFailed means Marshal() returned an error
	MarshalFailed

	// UnmarshalFailed means Unmarshal() returned an error
	UnmarshalFailed

	// Unimplemented means flow hit a point requiring deeper implementation
	Unimplemented

	// FileSysError means an unexpected file sys error occured
	FileSysError

	// ConfigNotRead denotes that the given file was not found/read
	ConfigNotRead

	// ServiceShutdown means the host service is either shutting down is shutdown
	ServiceShutdown

	/*****************************************************
	** Repo
	**/

	// RepoEntryErrorFamily errors generally relate to prepo
	RepoEntryErrorFamily = 5100 + iota

	// BadPDIEntryFormat means the PDI entry being processed is corrupted or was created using an unsupported format
	BadPDIEntryFormat

	// CommunityNotFound means the specified community name or ID did not match any of the registered communities
	CommunityNotFound

	// CommunityEpochNotFound means the community epoch ID cited was not found in this repo.
	CommunityEpochNotFound

	// FailedToConnectStorageProvider means the connection attempt the StorageProvider failed
	FailedToConnectStorageProvider

    // MemberEpochNotFound means the given member ID and/or member epoch num was not found
    MemberEpochNotFound
    
	// ChannelNotFound means the given ChannelID was not found in the community repo
	ChannelNotFound

    // ChannelSessionNotFound means the given channel session ID did not reference an active channel session.
    ChannelSessionNotFound

	// FailedToLoadChannel means a channel failed to load all its files from its host community repo
	FailedToLoadChannel

    // ChAgentNotFound means the requested channel agent was not found
    ChAgentNotFound

	// InvalidEntrySignature means the entry did not match the signature computed for the given entry body and the author's corresponding verify sig
	InvalidEntrySignature

	// AuthorNotFound means the given author was not found in the given access control list.
	AuthorNotFound

	// ACCNotFound means the access channel specified by a given PDI entry was not found
	ACCNotFound

	// NotAnACC means the channel ID cited by a PDI entry was not actually an access channel
	NotAnACC

	// FailedToProcessPDIHeader means decryption or unmarshalling of a PDI failed
	FailedToProcessPDIHeader

	// ChEntryIsMalformed means decryption or unmarshalling of a pdi channel entry failed
	ChEntryIsMalformed

	// ChEntryNotMerged means the given entry cannot yet be merged and must wait
	ChEntryNotMerged

	// AuthorLacksWritePermission means the given PDI entry's author does not have write permission to the specified channel
	AuthorLacksWritePermission

	// BadTimestamp means a timestamp is in the excessively distant past or future
	BadTimestamp

	// ChannelEpochNotFound means the cited epoch of the target channel did not match any known epochs locally.
	ChannelEpochNotFound

    // ChannelEpochNotLive means the cited channel epoch is not currently live
    ChannelEpochNotLive

    // ChannelEpochDisallows means the entry does not conform to its cited channel epoch
    ChannelEpochDisallows

    // ChannelEpochExpired means the cited channel epoch has been superseded and does not allow the given entry.
    ChannelEpochExpired

    // GenesisEntryNotVerified means an entry was marked as a genesis entry but could not be verified.
    GenesisEntryNotVerified

	// TxnDBNotReady means the txnDB failed to read or write txn data
	TxnDBNotReady

	// TxnNotConsistent means info in one or more txns is at odds, meaning malicious txn packaging may be in play
	TxnNotConsistent

    // StorageNotConsistent means the storae provider returned info that is suspect or inconsistent
    StorageNotConsistent

	// TxnDecodeFailed means the given txn failed to be decoded
	TxnDecodeFailed

	// UnsupportedPayloadCodec means the given txn payload codec type is not recognized or supported
	UnsupportedPayloadCodec

	// CannotExtractTxnPayload means the txn payload failed to be extracted
	CannotExtractTxnPayload

	/*****************************************************
	** SKI / Security
	**/

	// SecurityErrorFamily errors relate to PLAN's Secure Key Interface (SKI)
	SecurityErrorFamily = 5200 + iota

    // KeyringNotFound means the given key set was not found
    KeyringNotFound

	// KeyEntryNotFound means a key source did not contain the requested pub key (or pub key fragment)
	KeyEntryNotFound

	// InvocationNotAvailable means an SKI session was started with an unrecognized invocation string
	InvocationNotAvailable

	// InvocationAlreadyExists means an ski.Provider has already been registered with the given invocation string
	InvocationAlreadyExists

	// CryptoKitIDAlreadyRegistered  means the given package ID was already registered
	CryptoKitIDAlreadyRegistered

	// CryptoKitNotFound means the requested CryptoKitID was not registered for any CryptoKitID
	CryptoKitNotFound

	// HashKitNotFound means the requested HashKitID was not found
	HashKitNotFound

	// KeyGenerationFailed means key generation failed
	KeyGenerationFailed

	// KeyringNotSpecified means no keyring scope name was given for the SKI operation
	KeyringNotSpecified

	// CommunityNotSpecified means inArgs.KeySpecs.CommunityID was not set (and so the op can't proceed)
	CommunityNotSpecified

	// UnknownSKIOpName means the given SKI op name was not recognized
	UnknownSKIOpName

	// InsufficientSKIAccess means the requested permissions were not issued to allow the operation to proceed
	InsufficientSKIAccess

	// InvalidSKISession means the given session is not currently open
	InvalidSKISession

	// KeyIDCollision occurs when a key is placed in a keyring that already contains the key ID
	KeyIDCollision

	// KeyImportFailed means one or more keys that tried to be imported failed.
	KeyImportFailed

	// BadKeyFormat means key data was a length or format that was invalid or unexpected
	BadKeyFormat

	// FailedToDecryptKeyImport means a PDI entry body content failed to decrypt
	FailedToDecryptKeyImport

	// FailedToProcessKeyImport means that an error occurred while processing a PDI key import
	FailedToProcessKeyImport

	// FailedToMarshalKeyExport means an error occurred while encoding a PDI key export
	FailedToMarshalKeyExport

	// FailedToDecryptData means either the key or buffer to to be decrypted failed verification,
	FailedToDecryptData

	// VerifySignatureFailed means either the given signature did not match the given digest.
	VerifySignatureFailed

	// KeyTomeFailedToLoad means the cold storage for a keyring failed to load or does not exist
	KeyTomeFailedToLoad

	// KeyTomeFailedToWrite means an error occured while trying to write a key file
	KeyTomeFailedToWrite

	/*****************************************************
	** StorageProvider
	**/

	// StorageErrorFamily errors relate to PLAN's PDI Storage abstraction
	StorageErrorFamily = 5300 + iota

	// InvalidStorageSession means the given storage session ID was not found
	InvalidStorageSession

	// InvalidDatabaseID means the database ID provided is suspiciously short or long
	InvalidDatabaseID

	// FailedToAccessPath means a pathname was unable to be created or otherwise accessed
	FailedToAccessPath

	// FailedToLoadDatabase means an error was encountered when creating or loading the database
	FailedToLoadDatabase

	// FailedToCommitTxn means an unexpected fatal error occurred while committing one ore more txns
	FailedToCommitTxn

	// IncompatibleStorage means the given TxnEncoder desc string reflects that the SP and client are incompatible
	IncompatibleStorage

	// EncoderSessionNotReady means that TxnEncoder.ResetSession() has either not yet been called or the SKI session associated with it has closed.
	EncoderSessionNotReady

	// StorageImplNotFound means the requested storage impl is not available, etc.
	StorageImplNotFound

	// TxnPartsMissing means one or more required txn parts are not present
	TxnPartsMissing

	// AccountNotAvailable means the StorageProvider failed to find the account associated w/ the txn's "from" public key (or is damaged)
	AccountNotAvailable

	// InsufficientPostage means the sender does not have a balance to commit their txn
	InsufficientPostage

	// TransferFailed means the gas/fiat transfer failed (e.g. insufficient funds)
	TransferFailed

	// StorageNotReady means a problem occurred at the storage level
	StorageNotReady

	// NetworkNotReady means the network is in an unusable state
	NetworkNotReady

	// ConfigFailure means something went wrong loading/reading config
	ConfigFailure

	// TxnQueryFailed means the txn query failed to execute
	TxnQueryFailed

	// TxnFailedToDecode means TxnDecoder.DecodeRawTxn() returned an error
	TxnFailedToDecode

	/*****************************************************
	** Network
	**/

	// NetworkErrorFamily errors generally relate to network services and connections
	NetworkErrorFamily = 5400 + iota

	// SessionTokenMissing means a session token was not found in the request context
	SessionTokenMissing

	// SessionTokenNotValid means the session token that was supplied didn't match anything
	SessionTokenNotValid
)
