package pdi

import (
	//"fmt"
	//"sync"

	//"google.golang.org/grpc/encoding"

	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"
)

/**********************************************************************************************************************
  StorageProvider wraps a persistent storage service provider.

  See the service defintion for StorageProvider, located in go-plan/pdi/pdi.proto.

  The TxnEncoder/TxnDecoder model is designed to wrap any kind of append-only database,
      particularly a distributed ledger.

*/

// TxnSegmentMaxSz allows malicious-sized txns to be detected
const TxnSegmentMaxSz = 10 * 1024 * 1024

// TxnEncoder encodes arbitrary data payloads into storage txns native to a specific StorageProvider.
// For each StorageProvider implementation, there is a corresponding TxnEncoder that allows
//     native txns to be generated locally for submission to the remote StorageProvider.
// The StorageProvider + txn Encoder/Decoder system preserves the property that a StorageProvider must operate deterministically,
//     and can only validate txns and maintain a ledger of which public keys can post (and how much).
// TxnEncoder is NOT assumed to be threadsafe unless specified otherswise
type TxnEncoder interface {

	// ResetSigner -- resets how this TxnEncoder signs newly encoded txns in EncodeToTxns().
    // inFrom is the pub key created via StorageEpoch.GenerateNewAddr().  
    // If len(inFrom) == 0, the latest key on the StorageEpoch's keyring name is used. 
	ResetSigner(
		inSession ski.Session,
		inFrom    []byte,
	) error

	// EncodeToTxns encodes the payload and payload codec into one or more native and signed StorageProvider txns.
	// Pre: ResetSigner() must be successfully called.
	EncodeToTxns(
		inPayloadData     []byte,
		inPayloadEncoding plan.Encoding,
		inTransfers       []*Transfer,
		inTimeSealed      int64, // If non-zero, this is used in place of the current time
	) (*PayloadTxnSet, error)

	// Generates a txn that destroys the given address from committing any further txns.
	//EncodeDestruct(from ski.PubKey) (*Txn, error)
}

// TxnDecoder decodes storage txns native to a specific remote StorageProvider into the original payloads.
// TxnDecoder is NOT assumed to be threadsafes unless specified otherswise
type TxnDecoder interface {

	// Decodes a raw txn from a StorageProvider (from a corresponding TxnEncoder)
	// Also performs signature validation on the given txn, meaning that if no err is returned,
	//    then the txn was indeed signed by outInfo.From.
	// Returns the payload buffer segment buf.
	DecodeRawTxn(
		inRawTxn []byte,   // Raw txn to be decoded
		outInfo  *TxnInfo, // If non-nil, populated w/ info extracted from inTxn
	) ([]byte, error)
}


// StorageKeyringSz is the byte size of the name used to identify a Keyring used for StorageProvider keys
const StorageKeyringSz = 18


// StorageKeyringName returns the binary name for this StorageEpoch, formed from its origin key.
//
// Collisions are generally not possible since a StorageEpoch is scoped to a given community ID.
// Hence, a community ID carries the "heavy lifting" of uniqueness.
func (epoch *StorageEpoch) StorageKeyringName() []byte {

    keyInfo := epoch.OriginKey
    if keyInfo == nil || len(keyInfo.PubKey) == 0 || keyInfo.CryptoKit == 0 || keyInfo.KeyType != ski.KeyType_SigningKey {
        return nil
    }

    sz := uint32(len(keyInfo.PubKey))
    if sz > StorageKeyringSz {
        sz = StorageKeyringSz
    }

    return keyInfo.PubKey[:sz]
}

// CommunityChID returns the ChID of the requested community-global channel.
func (epoch *StorageEpoch) CommunityChID(inCommunityChID CommunityChID) plan.ChID {
    pos := inCommunityChID * plan.ChIDSz
    return epoch.CommunityChIDs[pos:pos+plan.ChIDSz]
}

// RootACC returns the ChID of the community's root ACC
func (epoch *StorageEpoch) RootACC() plan.ChID {
    return epoch.CommunityChID(CommunityChID_RootACC)
}

// MemberRegistry returns the ChID of the community's member registry channel.
func (epoch *StorageEpoch) MemberRegistry() plan.ChID {
    return epoch.CommunityChID(CommunityChID_MemberRegistry)
}

// CommunityEpochs returns the ChID of the community's CommunityEpoch history channel
func (epoch *StorageEpoch) CommunityEpochs() plan.ChID {
    return epoch.CommunityChID(CommunityChID_EpochHistory)
}

// GenerateNewAddr generates a new signing key on the given SKI session for this StorageEpoch,
//    returning the newly generated pub key (used as an address on a StorageProvider network). 
func (epoch *StorageEpoch) GenerateNewAddr(
    inSession ski.Session,
) ([]byte, error) {

    krName := epoch.StorageKeyringName()

    if len(krName) == 0 {
        return nil, plan.Error(nil, plan.ParamMissing, "invalid StorageEpoch")
    }

    keyInfo, err := ski.GenerateNewKey(
        inSession,
        krName,
        ski.KeyInfo{
            KeyType:   epoch.OriginKey.KeyType,
            CryptoKit: epoch.OriginKey.CryptoKit,
        },
    )
    if err != nil {
        return nil, err
    }

    return keyInfo.PubKey, nil
}

// CommunityKeyringName returns the name of the community keyring name
func (epoch *StorageEpoch) CommunityKeyringName() []byte {
    return epoch.CommunityID
}

// FormSuggestedDirName forms a file system friendly name that identifies this community to humans.
func (epoch *StorageEpoch) FormSuggestedDirName() string {
    str := plan.MakeFSFriendly(
        epoch.Name,
        epoch.CommunityID[0:2],
    )

    return str
}
