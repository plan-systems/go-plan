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





// SessionTool is a small set of util functions for creating a SKI session.
type SessionTool struct {
    UserName    string
    Session     Session
    CommunityID []byte

    blocker     chan int   
}





// NewSessionTool creates a new tool for helping manage a SKI session.
func NewSessionTool(
    inProvider Provider,
    inUserName string,
    inCommunityID []byte,   // if len()==0, it will be auto-generated
) (*SessionTool, *plan.Perror) {


    st := &SessionTool{
        UserName: inUserName,
        CommunityID: inCommunityID,
        blocker: make(chan int, 1),
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
func (st *SessionTool) DoOp(inOpArgs OpArgs) (*plan.Block, *plan.Perror) {

    var outErr *plan.Perror
    var outResults *plan.Block

    st.Session.DispatchOp(inOpArgs, func(opResults *plan.Block, inErr *plan.Perror) {
        outErr = inErr
        outResults = opResults

        st.blocker <- 1
    })

    <- st.blocker


    return outResults, outErr
}





// GenerateNewKey creates a new key, blocking until completion
func (st *SessionTool) GenerateNewKey(
    inKeyType KeyType,
    inKeyDomain KeyDomain,
) *PubKey {

    var newKey *PubKey

    GenerateKeys(
        st.Session, 
        st.CommunityID[:], 
        []*PubKey{
            &PubKey{
                KeyType: inKeyType,
                KeyDomain: inKeyDomain,
            },
        },
        func(inKeys []*KeyEntry, inErr *plan.Perror) {
            if inErr != nil {
                log.Fatal(inErr)
            } else {
                newKey = &PubKey{
                    KeyDomain: inKeys[0].KeyDomain,
                    CryptoKitId: inKeys[0].CryptoKitId,
                    KeyBase: inKeys[0].PubKey,
                }
            }

            st.blocker <- 1
        },
    )

    <- st.blocker

    return newKey

}


// EndSession ends the current session
func (st *SessionTool) EndSession(inReason string) {

    st.Session.EndSession(inReason, func(inParam interface{}, inErr *plan.Perror) {
        if inErr != nil {
            log.Fatal(inErr)
        }
        st.blocker <- 1
    })

    <- st.blocker

}
