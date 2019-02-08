package datastore

import (
	"os"
	"bytes"
	"math/rand"
	"testing"

	"github.com/plan-systems/go-plan/pdi"
	"github.com/plan-systems/go-plan/plan"
	"github.com/plan-systems/go-plan/ski"

	"github.com/plan-systems/go-plan/ski/Providers/hive"
)

var gTestBuf = "May PLAN empower organizations and individuals, and may it be an instrument of productivity and self-organization."

var gTesting *testing.T
var gCommunityID = [4]byte{0, 1, 2, 3}
var gDefaultFileMode = os.FileMode(0775)

func TestVarAppendBuf(t *testing.T) {

	gTesting = t

	seed := plan.Now().UnixSecs
	gTesting.Logf("Using seed: %d", seed)
	rand.Seed(seed)

	testBufs := make([][]byte, 100)

	for i := range testBufs {
		N := int(1 + rand.Int31n(int32(len(gTestBuf))-2))
		testBufs[i] = []byte(gTestBuf[:N])
	}

	buf := make([]byte, len(testBufs)*len(gTestBuf))
	totalLen := 0

	var err error
	for i := range testBufs {
		totalLen, err = pdi.AppendVarBuf(buf, totalLen, testBufs[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	buf = buf[:totalLen]

	offset := 0
	var payload []byte

	for i := range testBufs {
		offset, payload, err = pdi.ReadVarBuf(buf, offset)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(payload, testBufs[i]) != 0 {
			t.Fatalf("'%v' != '%v'", string(payload), string(testBufs[i]))
		}
	}

	if offset != len(buf) {
		t.Fatalf("expected offset == %v, got %v'", len(buf), offset)
	}
}

func TestFileSysSKI(t *testing.T) {

	gTesting = t

	// Register providers to test
	encodersToTest := []func() (pdi.TxnEncoder, pdi.TxnDecoder){
		func() (pdi.TxnEncoder, pdi.TxnDecoder) {
			decoder := NewTxnDecoder()
			encoder, _ := NewTxnEncoder(1000)
			return encoder, decoder
		},
	}

	tool, err := ski.NewSessionTool(
		hive.NewProvider(),
		"Test-Encoding",
		gCommunityID[:],
	)
	if err != nil {
		gTesting.Fatal(err)
	}

	A := &testSession{
		*tool,
		nil,
		nil,
	}

	for _, createCoders := range encodersToTest {

		A.encoder, A.decoder = createCoders()

		txnEncodingTest(A)

		A.EndSession("done A")
	}
}

func txnEncodingTest(A *testSession) {

	// Test agent encode/decode
	{
		blobBuf := make([]byte, 500000)
		decoder := NewTxnDecoder()
		encoder, _ := NewTxnEncoder(1000)

		{
			err := encoder.ResetSession(
				decoder.EncodingDesc(),
				A.Session,
				gCommunityID[:],
			)
			if err != nil {
				gTesting.Fatal(err)
			}

			pubKey, err := encoder.GenerateNewAccount()
			if err != nil {
				gTesting.Fatal(err)
			}

			err = encoder.ResetAuthorID(*pubKey)
			if err != nil {
				gTesting.Fatal(err)
			}
		}

		for i := 0; i < 100; i++ {

			blobLen := int(rand.Int31n(1 + rand.Int31n(5000)))

			payload := blobBuf[:blobLen]
			rand.Read(payload)

			txns, err := encoder.EncodeToTxns(
				payload,
				[]byte{4, 3, 2, 1},
				pdi.PayloadCodec_Unspecified,
				nil,
			)
			if err != nil {
				gTesting.Fatal(err)
			}

			for _, txn := range txns {
				decodedInfo := pdi.TxnInfo{}
				decodedSeg := pdi.TxnSegment{}

				err := decoder.DecodeRawTxn(
					txn.RawTxn,
					&decodedInfo,
					&decodedSeg,
				)
				if err != nil {
					gTesting.Fatal(err)
				}

				b1, _ := decodedInfo.Marshal()
				b2, _ := txn.TxnInfo.Marshal()
				if bytes.Compare(b1, b2) != 0 {
					gTesting.Fatal("txn seg info check failed")
				}
				//if Bytes.Compare(decodedSeg.SegData, txn.
			}
		}
	}
}

type testSession struct {
	ski.SessionTool

	encoder pdi.TxnEncoder
	decoder pdi.TxnDecoder
}
