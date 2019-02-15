package ski

import (
    "io"
	"bytes"
    "hash"
    "log"
    "crypto/rand"

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

// CopyToPubKey copies this KeyEntry into the form of a PubKey
func (entry *KeyEntry) CopyToPubKey() *PubKey {

    return &PubKey{
        KeyType: entry.KeyType,
        KeyDomain: entry.KeyDomain,
        CryptoKitId: entry.CryptoKitId,
        Bytes: entry.PubKey,
    }
}

// CopyFrom copies the fields from the given KeyEntry into this PubKey
func (pk *PubKey) CopyFrom(entry *KeyEntry) {
    
    pk.KeyType = entry.KeyType
    pk.KeyDomain = entry.KeyDomain
    pk.CryptoKitId = entry.CryptoKitId
    pk.Bytes = entry.PubKey
}




// NewHashKit returns the requested HashKit.
func NewHashKit(inID HashKitID) (HashKit, error) {

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
) ([]*KeyEntry, error) {

    N :=  len(inKeySpecs)

    newKeys := make([]*KeyEntry, N)

    var kit *CryptoKit 
    var err error

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
) ([]*KeyEntry, error) {

    results, err := skiSession.DoOp( OpArgs{
        OpName: OpGenerateKeys,
        CommunityID: inCommunityID,
        KeySpecs: inKeySpecs,
    })

    var newKeys []*KeyEntry

    if err == nil {
        bundleBuf := results.GetContentWithCodec(KeyBundleProtobufCodec, 0)
        keyBundle := KeyBundle{}
        err = keyBundle.Unmarshal(bundleBuf)   
        if err != nil {
            err = plan.Error(err, plan.FailedToUnmarshal, "failed to unmarshal KeyBundle from OpGenerateKeys")
        } else {
            newKeys = keyBundle.Keys

            N := len(keyBundle.Keys)
            plan.Assert(N == len(inKeySpecs), "number of keys returned from GenerateKeys() does not match input")

            for i := 0; i < N; i++ {
                plan.Assert(keyBundle.Keys[i].KeyType == inKeySpecs[i].KeyType, "keys generated from GenerateKeys() don't match request")
            }
        }
    }

    if err != nil {
        return nil, err
    }

    return newKeys, nil

}





// SessionTool is a small set of util functions for creating a SKI session.
type SessionTool struct {
    UserName    string
    Session     Session
    CommunityID []byte

    blocker     chan error   
}





// NewSessionTool creates a new tool for helping manage a SKI session.
func NewSessionTool(
    inProvider Provider,
    inUserName string,
    inCommunityID []byte,   // if len()==0, it will be auto-generated
) (*SessionTool, error) {


    st := &SessionTool{
        UserName: inUserName,
        CommunityID: inCommunityID,
        blocker: make(chan error, 1),
    }

    if len(st.CommunityID) == 0 {
        st.CommunityID = make([]byte, plan.CommunityIDSz)
        rand.Read(st.CommunityID)
    }

    path, err := plan.UseLocalDir(inUserName)
    if err != nil {
        return nil, err
    }
    
    st.Session, err = inProvider.StartSession(SessionParams{
        Invocation: plan.Block{
            Label: inProvider.InvocationStr(),
        },
        BaseDir: path,
    })

    return st, err
}



// DoOp performs the given op, blocking until completion
func (st *SessionTool) DoOp(inOpArgs OpArgs) (*plan.Block, error) {

    results, err := st.Session.DoOp(inOpArgs)

    return results, err
}





// GenerateNewKey creates a new key, blocking until completion
func (st *SessionTool) GenerateNewKey(
    inKeyType KeyType,
    inKeyDomain KeyDomain,
) *PubKey {

    newKeys, err := GenerateKeys(
        st.Session, 
        st.CommunityID[:], 
        []*PubKey{
            &PubKey{
                KeyType: inKeyType,
                KeyDomain: inKeyDomain,
            },
        },
    )

    if err != nil {
        log.Fatal(err)
    }

    return newKeys[0].CopyToPubKey()
}


// EndSession ends the current session
func (st *SessionTool) EndSession(inReason string) {

    st.Session.EndSession(inReason, func(inParam interface{}, inErr error) {
        st.blocker <- inErr
    })

    err := <- st.blocker
    if err != nil {
        log.Fatal(err)
    }


}
