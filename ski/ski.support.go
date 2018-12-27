package ski

import (
    "io"
	"bytes"

	"github.com/plan-systems/go-plan/plan"
)


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


// GenerateNewKeys is a convenience bulk function for CryptoKit.GenerateNewKey()
func GenerateNewKeys(
    inRand io.Reader,
    inRequestedKeyLen int,
    inKeyReqs []*KeyEntry,
) ([]*KeyEntry, *plan.Perror) {

    N :=  len(inKeyReqs)

    newKeys := make([]*KeyEntry, N)

    var kit *CryptoKit 
    var err *plan.Perror

    timeCreated := plan.Now().UnixSecs

    for i, keyReq := range inKeyReqs {

        if kit == nil || kit.CryptoKitID != keyReq.CryptoKitId {
            kit, err = GetCryptoKit(keyReq.CryptoKitId)
            if err != nil {
                return nil, err
            }
        }

        newKey := &KeyEntry{
            KeyType: keyReq.KeyType,
            KeyDomain: keyReq.KeyDomain,
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
    inKeySpecs []*KeyEntry,
    inOnCompletion func(inKeys []*KeyEntry, inErr *plan.Perror),
) {

    skiSession.DispatchOp(&OpArgs{
            OpName: OpGenerateKeys,
            KeySpecs: KeyBundle{
                CommunityId: inCommunityID,
                Keys: inKeySpecs,
            },
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