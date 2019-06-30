package pdi

import (
	//crypto_rand "crypto/rand"
    //"os"
    //"path"
    //"io/ioutil"
    //"strings"
    //"sync"
    //"time"
    //"sort"
    //"encoding/hex"
    //"encoding/json"
    //"context"
    //"fmt"
    

    "github.com/plan-systems/plan-core/tools"
    "github.com/plan-systems/plan-core/plan"
    "github.com/plan-systems/plan-core/ski"

    //"github.com/dgraph-io/badger"

)

// MemberCrypto instances a personal SKI session that can encode ans sign new entries, etc.
type MemberCrypto struct {

    // Allows key callback functionality 
    //Host            MemberHost

    skiSession      ski.Session

    TxnEncoder      TxnEncoder

    StorageEpoch    StorageEpoch
    CommunityEpoch  CommunityEpoch
    MemberEpoch     MemberEpoch
    MemberIDStr     string

    Packer          ski.PayloadPacker

    communityKey    ski.KeyRef

    // packing scrap
    tmpCrypt        EntryCrypt
    tmpInfo         EntryInfo

    tmpBuf          []byte
}



func (mc *MemberCrypto) StartSession(
    inSKISession   ski.Session,
) error {

    mc.communityKey = mc.CommunityEpoch.CommunityKeyRef()
    mc.skiSession = inSKISession

    // Set up the entry packer using the singing key associated w/ the member's current MemberEpoch
    mc.Packer = ski.NewPacker(false)
    err := mc.Packer.ResetSession(
        mc.skiSession,
        ski.KeyRef{
            KeyringName: mc.MemberEpoch.FormSigningKeyringName(mc.CommunityEpoch.CommunityID),
            PubKey: mc.MemberEpoch.PubSigningKey,
        },
        mc.CommunityEpoch.EntryHashKit,
        nil,
    )
    if err != nil { return err }


    // Use the member's latest txn/storage signing key.
    if err = mc.TxnEncoder.ResetSigner(mc.skiSession, nil); err != nil { 
        return err
    }

    return nil
}

func (mc *MemberCrypto) EndSession(inReason string) {
    if mc.skiSession != nil {
        mc.skiSession.EndSession(inReason)
        mc.skiSession = nil
    }
}


// EncryptAndEncodeEntry encodes entry.tmpInfo and entry.Body into txns, filling out entry.PayloadTxnSet
func (mc *MemberCrypto) EncryptAndEncodeEntry(
    ioInfo *EntryInfo,
    inBody []byte,
) (*PayloadTxnSet, error) {

    // Should already be nil
    ioInfo.AuthorSig = nil

    timeAuthored := ioInfo.TimeAuthored()
    if timeAuthored <= 0 {
        return nil, plan.Error(nil, plan.ParamErr, "entry time authored not set")
    }

    mc.tmpBuf = tools.SmartMarshal(ioInfo, mc.tmpBuf)

    // Have the member sign the header
    var packingInfo ski.PackingInfo
    err := mc.Packer.PackAndSign(
        plan.Encoding_Pb_EntryInfo,
        mc.tmpBuf,
        inBody,
        0,
        &packingInfo,
    )
    if err != nil {
        return nil, err
    }

    // With the entry sealed, we can now form its TID.
    ioInfo.EntryID().SetHash(packingInfo.Hash)

    // TODO: use and check scrap mem (prevent overwrites, etc)
    packedEntry, err := mc.CommunityEncrypt(packingInfo.SignedBuf)
    if err != nil {
        return nil, err
    }

    tmpCrypt := EntryCrypt{
        CommunityEpochID: mc.CommunityEpoch.EpochTID,
        PackedEntry: packedEntry,
    }

    mc.tmpBuf = tools.SmartMarshal(&tmpCrypt, mc.tmpBuf)

    payloadTxnSet, err := mc.TxnEncoder.EncodeToTxns(
        mc.tmpBuf,
        plan.Encoding_Pb_EntryCrypt,
        nil,
        timeAuthored,
    )

    if err != nil {
        return nil, err
    }

    return payloadTxnSet, nil
}



func (mc *MemberCrypto) CommunityEncrypt(
    inBuf    []byte,
) ([]byte, error) {

    out, err := mc.skiSession.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_ENCRYPT_SYM,
        OpKey: &mc.communityKey,
        BufIn: inBuf,
    })
    if err != nil {
        return nil, err
    }

    return out.BufOut, nil
}


func (mc *MemberCrypto) ExportCommunityKeyring(
    inPass []byte,
) ([]byte, error) {

    out, err := mc.skiSession.DoCryptOp(&ski.CryptOpArgs{
        CryptOp: ski.CryptOp_EXPORT_USING_PW,
        PeerKey: inPass,
        TomeIn: &ski.KeyTome{
            Keyrings: []*ski.Keyring{
                &ski.Keyring{
                    Name: mc.communityKey.KeyringName,
                },
            },
        },
    })

    if err != nil {
        return nil, err
    }

    return out.BufOut, nil
}



