package plan

import (
	"fmt"
	"strings"
)

// Assert asserts essential assumptions
func Assert(inCond bool, inFormat string, inArgs ...interface{}) {

	if !inCond {
		panic(fmt.Sprintf(inFormat, inArgs...))
	}
}

/*****************************************************
** Err / plan.Error()
**/

// GUI error philosophy: errors can be suppressed by type or by item that they are for.

// Err is PLAN's common error struct.  Err.Code allows easy matching while allowing error strings to contain useful contextual information.
type Err struct {  
    Status  

	Err  error
}

// Error create a new PError
func Error(inErr error, inCode int32, inMsg string) *Err {
	return &Err{
		Status{
            inCode,
            inMsg,
            nil,
        },
		inErr,
	}
}

// Errorf is a convenience function of Error() that uses a string formatter.
func Errorf(inErr error, inCode int32, inFormat string, inArgs ...interface{}) *Err {
	return &Err{
		Status{
            inCode,
            fmt.Sprintf(inFormat, inArgs...),
            nil,
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

const (

	/*****************************************************
	** Universal errors
	**/

	// GenericErrorFamily errors generally relate to pnode
	GenericErrorFamily = 5000 + iota

	// AssertFailed means an unreachable part of code was...reached.  :\
	AssertFailed

	// MissingParam one or more params was missing, nil, or not otherwise given
	MissingParam

    // FailedToMarshal means Marshal() returned an error
    FailedToMarshal

    // FailedToUnmarshal means Unmarshal() returned an error
    FailedToUnmarshal

    // Unimplemented means flow hit a point requiring deeper implementation
    Unimplemented

    // FileSysError means an unexpected file sys error occured
    FileSysError

    // ConfigNotRead denotes that the given file was not found/read
    ConfigNotRead

	/*****************************************************
	** PDI
	**/

	// PDIEntryErrorFamily errors generally relate to pnode
	PDIEntryErrorFamily = 5100 + iota

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

	// TargetChannelEpochNotFound means the cited epoch of the target channel did not match any known epochs locally.
	TargetChannelEpochNotFound

	// TargetChannelEpochExpired means an entry cited a target channel epoch that has expired
	TargetChannelEpochExpired



    /*****************************************************
	** SKI / Security
	**/

	// SecurityErrorFamily errors relate to PLAN's Secure Key Interface (SKI)
	SecurityErrorFamily = 5200 + iota

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

	// KeyringNotFound means the given keyring name was not found
	KeyringNotFound

	// KeyDomainNotFound means the KeyDomain given was not known (and out of range)
	KeyDomainNotFound

    // CommunityNotSpecified means inArgs.KeySpecs.CommunityID was not set (and so the op can't proceed)
    CommunityNotSpecified

	// UnknownSKIOpName means the given SKI op name was not recognized
	UnknownSKIOpName

	// InsufficientSKIAccess means the requested permissions were not issued to allow the operation to proceed
	InsufficientSKIAccess

	// InvalidSKISession means the given session is not currently open
	InvalidSKISession

	// KeyEntryNotFound means a key source did not contain the requested key ID
	KeyEntryNotFound

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
	** StorageSession / StorageProvider
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

    // InsufficientGas means the sender does not have a balance with sufficient gas to commit the txn
    InsufficientGas

    // TransferFailed means the gas/fiat transfer failed (e.g. insufficient funds)
    TransferFailed

    // StorageNotReady means a problem occurred at the storage level
    StorageNotReady

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
