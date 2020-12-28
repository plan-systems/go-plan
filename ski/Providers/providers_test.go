package main

import (
	"bytes"
	"math/rand"

	"testing"

	"github.com/plan-systems/plan-go/bufs"
	"github.com/plan-systems/plan-go/ctx"
	"github.com/plan-systems/plan-go/ski"

	//"github.com/plan-systems/plan-go/plan"

	"github.com/plan-systems/plan-go/ski/Providers/hive"
)

var gTesting *testing.T
var gCommunityID = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

func TestFileSysSKI(t *testing.T) {
	gTesting = t

	{
		A := newSession("Alice")
		B := newSession("Bob")

		doProviderTest(A, B)

		A.EndSession("done A")
		B.EndSession("done B")
	}
}

func doProviderTest(A, B *SessionTool) {

	// 1) Generate a new community key (on A)
	err := A.GetLatestKey(&A.CommunityKey, ski.KeyType_SymmetricKey)
	if err != nil {
		gTesting.Fatal(err)
	}

	// 2) export the community key from A
	opBuf := A.doOp(ski.CryptOpArgs{
		CryptOp: ski.CryptOp_ExportToPeer,
		OpKey:   &A.P2PKey,
		PeerKey: B.P2PKey.PubKey,
		TomeIn: &ski.KeyTome{
			Keyrings: []*ski.Keyring{
				&ski.Keyring{
					Name: A.CommunityKey.KeyringName,
				},
			},
		},
	})

	// 3) insert the new community key into B
	opBuf = B.doOp(ski.CryptOpArgs{
		CryptOp: ski.CryptOp_ImportFromPeer,
		BufIn:   opBuf,
		OpKey:   &B.P2PKey,
		PeerKey: A.P2PKey.PubKey,
	})

	clearMsg := []byte("hello, PLAN community!")

	// 4) Encrypt a new community msg (sent from A)
	opBuf = A.doOp(ski.CryptOpArgs{
		CryptOp: ski.CryptOp_EncryptSym,
		BufIn:   clearMsg,
		OpKey:   &A.CommunityKey,
	})

	encryptedAtoB := opBuf

	// 5) Send the encrypted community message to B
	opBuf = B.doOp(ski.CryptOpArgs{
		CryptOp: ski.CryptOp_DecryptSym,
		BufIn:   encryptedAtoB,
		OpKey:   &B.CommunityKey,
	})

	decryptedMsg := opBuf

	// 6) Now check that B can send an encrypted community msg to A
	opBuf = B.doOp(ski.CryptOpArgs{
		CryptOp: ski.CryptOp_EncryptSym,
		BufIn:   decryptedMsg,
		OpKey:   &B.CommunityKey,
	})
	encryptedBtoA := opBuf
	opBuf = A.doOp(ski.CryptOpArgs{
		CryptOp: ski.CryptOp_DecryptSym,
		BufIn:   encryptedBtoA,
		OpKey:   &A.CommunityKey,
	})

	// 7) Did the round trip work?
	//    clearMsg => A => encryptedAtoB => B => decryptedMsg => B => encryptedBtoA => A => opResults.Content
	if !bytes.Equal(clearMsg, opBuf) {
		gTesting.Fatalf("expected %v, got %v after decryption", clearMsg, opBuf)
	}

	badMsg := make([]byte, len(encryptedAtoB))

	// Vary the data slightly to test
	for i := 0; i < 1000; i++ {

		rndPos := rand.Int31n(int32(len(encryptedAtoB)))
		rndAdj := 1 + byte(rand.Int31n(254))
		copy(badMsg, encryptedAtoB)
		badMsg[rndPos] += rndAdj

		_, opErr := B.DoOp(ski.CryptOpArgs{
			CryptOp: ski.CryptOp_DecryptSym,
			BufIn:   badMsg,
			OpKey:   &B.CommunityKey,
		})
		if opErr == nil {
			gTesting.Fatal("there should have been a decryption error!")
		}
	}
}

// test setup helper
func newSession(inUserName string) *SessionTool {

	session, err := hive.StartSession("", "test", nil)
	if err != nil {
		gTesting.Fatal(err)
	}

	st, err := NewSessionTool(
		session,
		inUserName,
		gCommunityID[:],
	)
	if err != nil {
		gTesting.Fatal(err)
	}

	return st
}

// SessionTool is a small set of util functions for creating a SKI session.
type SessionTool struct {
	ctx.Logger

	UserID       string
	Session      ski.EnclaveSession
	CryptoKitID  ski.CryptoKitID
	CommunityKey ski.KeyRef
	P2PKey       ski.KeyRef
}

// NewSessionTool creates a new tool for helping manage a SKI enclave session.
func NewSessionTool(
	enclaveSess ski.EnclaveSession,
	userID string,
	communityID []byte,
) (*SessionTool, error) {

	st := &SessionTool{
		Logger:      ctx.NewLogger("ski_" + userID),
		UserID:      userID,
		Session:     enclaveSess,
		CryptoKitID: ski.CryptoKitID_NaCl,
		CommunityKey: ski.KeyRef{
			KeyringName: communityID,
		},
		P2PKey: ski.KeyRef{
			KeyringName: append([]byte(userID), communityID...),
		},
	}

	err := st.GetLatestKey(&st.P2PKey, ski.KeyType_AsymmetricKey)
	if err != nil {
		return nil, err
	}

	return st, nil
}

func (st *SessionTool) doOp(args ski.CryptOpArgs) []byte {
	results, err := st.DoOp(args)

	if err != nil {
		gTesting.Fatal(err)
	}
	return results
}

// DoOp performs the given op, blocking until completion
func (st *SessionTool) DoOp(args ski.CryptOpArgs) ([]byte, error) {
	out, err := st.Session.DoCryptOp(&args)
	if err != nil {
		return nil, err
	}

	return out.BufOut, nil
}

// GetLatestKey updates the given KeyRef with the newest pub key on a given keyring (using ioKeyRef.KeyringName)
func (st *SessionTool) GetLatestKey(
	ioKeyRef *ski.KeyRef,
	inAutoCreate ski.KeyType,
) error {

	ioKeyRef.PubKey = nil

	keyInfo, err := st.Session.FetchKeyInfo(ioKeyRef)
	if ski.IsError(err, ski.ErrCode_KeyringNotFound, ski.ErrCode_KeyEntryNotFound) {
		if inAutoCreate != ski.KeyType_Unspecified {
			keyInfo, err = ski.GenerateNewKey(
				st.Session,
				ioKeyRef.KeyringName,
				ski.KeyInfo{
					KeyType:     inAutoCreate,
					CryptoKitID: st.CryptoKitID,
				},
			)
			if err == nil {
				st.Infof(1, "created %v %v", inAutoCreate.String(), bufs.BufDesc(keyInfo.PubKey))
			}
		}
	}
	if err != nil {
		return err
	}

	ioKeyRef.PubKey = keyInfo.PubKey

	return nil
}

// EndSession ends the current session
func (st *SessionTool) EndSession(inReason string) {
	st.Session.EndSession(inReason)
}
