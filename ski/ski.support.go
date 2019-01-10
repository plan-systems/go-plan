package ski

import (
    "io"
	"bytes"
    "hash"

    "golang.org/x/crypto/sha3"

	"github.com/plan-systems/go-plan/plan"
)


// HashKit is an abstraction for hash.Hash
type HashKit struct {
    HashKitID HashKitID
    Hasher    hash.Hash
}

/*
type Hasher struct {
    kits     map[HashKitID]

}


func () ResetHashKit(inID HashKitID)  {

    if ID == 0 {
        inID = HashKitID_LegacyKeccak_256
    }


}
*/




// EqualTo compares if two key entries are identical/interchangable
func (entry *KeyEntry) EqualTo(other *KeyEntry) bool {
	return entry.KeyType != other.KeyType ||
		entry.KeyDomain != other.KeyDomain ||
		entry.CryptoKitId != other.CryptoKitId ||
		entry.TimeCreated != other.TimeCreated ||
		bytes.Equal(entry.PrivKey, other.PrivKey) == false ||
		bytes.Equal(entry.PubKey, other.PubKey) == false

}

// GetKeyID returns the KeyID for this KeyEntry
func (entry *KeyEntry) GetKeyID() plan.KeyID {
	return plan.GetKeyID(entry.PubKey)
}

// Base256 returns the pure binary representation of a key
func (pk *PubKey) Base256() []byte {
    if pk.Encoding == 0 {
        return pk.KeyBase
    }

    panic("TODO unimperr")
}


// NewHashKit returns the requested HashKit.
func NewHashKit(inID HashKitID) (HashKit, *plan.Perror) {

    var kit HashKit

    if inID == 0 {
        inID = HashKitID_LegacyKeccak_256
    }

    kit.HashKitID = inID

    switch inID {

        case 0, HashKitID_LegacyKeccak_256:
            kit.Hasher = sha3.NewLegacyKeccak256()

        case HashKitID_LegacyKeccak_512:
            kit.Hasher = sha3.NewLegacyKeccak512()

        case HashKitID_SHA3_256:
            kit.Hasher = sha3.New256()

        case HashKitID_SHA3_512:
            kit.Hasher = sha3.New512()

        default:
            return HashKit{}, plan.Errorf(nil, plan.HashKitNotFound, "failed to recognize HashKitID %v", inID)
    }

    return kit, nil
}



// GenerateNewKeys is a convenience bulk function for CryptoKit.GenerateNewKey()
func GenerateNewKeys(
    inRand io.Reader,
    inRequestedKeyLen int,
    inKeySpecs []*PubKey,
) ([]*KeyEntry, *plan.Perror) {

    N :=  len(inKeySpecs)

    newKeys := make([]*KeyEntry, N)

    var kit *CryptoKit 
    var err *plan.Perror

    timeCreated := plan.Now().UnixSecs

    for i, keySpec := range inKeySpecs {

        if kit == nil || kit.CryptoKitID != keySpec.CryptoKitId {
            kit, err = GetCryptoKit(keySpec.CryptoKitId)
            if err != nil {
                return nil, err
            }
        }

        newKey := &KeyEntry{
            KeyType: keySpec.KeyType,
            KeyDomain: keySpec.KeyDomain,
            CryptoKitId: kit.CryptoKitID,
            TimeCreated: timeCreated,
        }

        err = kit.GenerateNewKey(
            inRand,
            inRequestedKeyLen,
            newKey,
        )
        if err != nil {
            return nil, err
        }

        newKeys[i] = newKey
    }

    return newKeys, nil
}


// GenerateKeys is a convenience function that generates and returns keys via an open SKI session 
func GenerateKeys(
    skiSession Session,
    inCommunityID []byte,
    inKeySpecs []*PubKey,
    inOnCompletion func(inKeys []*KeyEntry, inErr *plan.Perror),
) {

    skiSession.DispatchOp( OpArgs{
            OpName: OpGenerateKeys,
            CommunityID: inCommunityID,
            KeySpecs: inKeySpecs,
        }, 
        func (inResults *plan.Block, err *plan.Perror) {
            var newKeys []*KeyEntry

            if err == nil {
                bundleBuf := inResults.GetContentWithCodec(KeyBundleProtobufCodec, 0)
                keyBundle := KeyBundle{}
                merr := keyBundle.Unmarshal(bundleBuf)   
                if merr != nil {
                    err = plan.Error(merr, plan.FailedToUnmarshal, "failed to unmarshal KeyBundle from OpGenerateKeys")
                } else {
                    newKeys = keyBundle.Keys

                    N := len(keyBundle.Keys)
                    plan.Assert(N == len(inKeySpecs), "number of keys returned from GenerateKeys() does not match input")

                    for i := 0; i < N; i++ {
                        plan.Assert(keyBundle.Keys[i].KeyType == inKeySpecs[i].KeyType, "keys generated from GenerateKeys() don't match request")
                    }
                }
            }

            inOnCompletion(newKeys, err)
        },
    )


}

/*
    results := ts.doOp(ski.OpArgs{
            OpName: ski.OpGenerateKeys,
            KeySpecs: ski.KeyBundle{
                CommunityId: gCommunityID[:],
                Keys: []*ski.KeyEntry{
                    &ski.KeyEntry{
                        KeyType: ski.KeyType_ASYMMETRIC_KEY,
                        KeyDomain: ski.KeyDomain_PERSONAL,
                    },
                    &ski.KeyEntry{
                        KeyType: ski.KeyType_SIGNING_KEY,
                        KeyDomain: ski.KeyDomain_PERSONAL,
                    },
                },
            },
        })


    {
        bundleBuf := results.GetContentWithCodec(ski.KeyBundleProtobufCodec, 0)
        keyBundle := ski.KeyBundle{}
        err := keyBundle.Unmarshal(bundleBuf)
        if err != nil {
            gTesting.Fatal(err)
        } 
        for i := 0; i < len(keyBundle.Keys); i++ {
            switch keyBundle.Keys[i].KeyType {
                case ski.KeyType_ASYMMETRIC_KEY:
                    ts.encryptPubKey = keyBundle.Keys[i].PubKey

                case ski.KeyType_SIGNING_KEY:
                    ts.signingPubKey = keyBundle.Keys[i].PubKey
            }
        }
    }
*/