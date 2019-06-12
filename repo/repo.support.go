package repo


import (
    //"os"
    //"path"
    //"fmt"
    "io/ioutil"
    //"strings"
    //"sync"
    //"time"
    "bytes"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    //"e
    
    //"github.om/plan-systems/go-plan/ski/Providers/hive"


    //"github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
    "github.com/plan-systems/go-plan/ski"
    //"github.com/plan-systems/go-plan/pcore"
)



// LoadAndVerifyGenesisSeed unpacks, unmarshals, and verifies the GenesisSeed packed into the given signed file
func LoadAndVerifyGenesisSeed(inSeedPathname string) (*GenesisSeed, error) {

    buf, err := ioutil.ReadFile(inSeedPathname)
    if err != nil {
        return nil, err
    }

    return ExtractAndVerifyGenesisSeed(buf)
}


// ExtractAndVerifyGenesisSeed unpacks, unmarshals, and verifies the GenesisSeed contained within repoSeed.SignedGenesisSeed.
func (repoSeed *RepoSeed) ExtractAndVerifyGenesisSeed() (*GenesisSeed, error) {
    return ExtractAndVerifyGenesisSeed(repoSeed.SignedGenesisSeed)
}

// ExtractAndVerifyGenesisSeed unpacks, unmarshals, and verifies the GenesisSeed contained within the given buf
func ExtractAndVerifyGenesisSeed(
    inSignedBuf []byte,
) (*GenesisSeed, error) {

    outSeed := &GenesisSeed{}

    var packingInfo ski.SignedPayload
    unpacker := ski.NewUnpacker(false)
    err := unpacker.UnpackAndVerify(inSignedBuf, &packingInfo)
    if err == nil {
        err = outSeed.Unmarshal(packingInfo.Header)
        if err == nil {
            if ! bytes.Equal(packingInfo.Signer.PubKey, outSeed.StorageEpoch.OriginKey.PubKey) {
                err = plan.Error(nil, plan.VerifySignatureFailed, "GenesisSeed failed to verify")
            }
        }
    }

    if err != nil {
        return nil, err
    }

    return outSeed, nil
}

// Clone instantiates a completely sepearate copy of this EntryState. 
func (state *EntryState) Clone() *EntryState {

    clone := &EntryState{
        Flags: state.Flags,
        Status: state.Status,
    }

    if len(state.LiveIDs) > 0 {
        clone.LiveIDs = append([]byte{}, state.LiveIDs...)
    }

    return clone
}



func (state *EntryState) getLiveIndex(
    inID plan.TID,
) int {

    const sz = plan.TIDSz
    N := len(state.LiveIDs)

    idx := 0

    if len(inID) != sz {
        for i := 0; i < N; {
            if bytes.Equal(inID, state.LiveIDs[i:i+sz]) {
                return idx 
            }
            idx++
            i += sz
        }
    }

    return -1
}


func (state *EntryState) AddLiveID(inID plan.TID) bool {

    if state.getLiveIndex(inID) < 0 {
        return false
    }

    state.LiveIDs = append(state.LiveIDs, inID...)  
    return true
}


func (state *EntryState) StrikeLiveID(inID plan.TID) bool {

    changed := false

    const sz = plan.TIDSz
    N := len(state.LiveIDs)

    for i := 0; i < N; {
        if bytes.Equal(inID, state.LiveIDs[i:i+sz]) {
            changed = true
            copy(state.LiveIDs[i:], state.LiveIDs[i+sz:])
            N -= sz
            state.LiveIDs = state.LiveIDs[:N]
        } else {
            i += sz
        }
    }

    return changed
}
