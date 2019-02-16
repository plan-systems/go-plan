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

func TestTxnEncoding(t *testing.T) {

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

    //rand.Seed(55055)

	// Test agent encode/decode
	{
        maxSegSize := int32(1000)
        
		blobBuf := make([]byte, 500000)
		decoder := NewTxnDecoder()
		encoder, _ := NewTxnEncoder(maxSegSize)

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


        txns := make([]*pdi.DecodedTxn, 10000)

        collator := pdi.NewTxnCollater()

		for i := 0; i < 1000; i++ {
            testTime := plan.Now().UnixSecs

			blobLen := int(rand.Int31n(1 + rand.Int31n(maxSegSize * 15)))

			payload := blobBuf[:blobLen]
			rand.Read(payload)

			rawTxns, err := encoder.EncodeToTxns(
				payload,
				pdi.PayloadCodec_Unspecified,
				nil,
                testTime + int64(i),
			)
			if err != nil {
				gTesting.Fatal(err)
			}

            gTesting.Logf("#%d: Testing %d segment txn set (payloadSz=%d)", i, len(rawTxns), len(payload))
            if i == 16 {
                err = nil
            }
            prevUTID := ""

			for idx, rawTxn := range rawTxns {
				decodedTxn := &pdi.DecodedTxn{}

				err = decodedTxn.DecodeRawTxn(
					rawTxn,
					decoder,
				)
				if err != nil {
					gTesting.Fatal(err)
                }
                if prevUTID != decodedTxn.Info.SegPrev {
                    gTesting.Fatal("prev seg UTID not set properly")
                }

                txns[idx] = decodedTxn
                prevUTID = decodedTxn.UTID
            }
            N := len(rawTxns)

            rand.Shuffle(N, func(i, j int) {
                txns[i], txns[j] = txns[j], txns[i]
            })

            var final *pdi.DecodedTxn
            for i := 0; i < N; i++ {
                final, err = collator.Desegment(txns[i])
                if err != nil {
                    gTesting.Fatal(err)
                }
                if final != nil && i < N - 1 {
                    gTesting.Fatal("got final too soon")
                }
            }

            if final == nil {
                gTesting.Fatal("didn't get final txn")
            }

            if bytes.Compare(payload, final.PayloadSeg) != 0 {
                gTesting.Fatal("payload failed")
            }

            if final.UTID != prevUTID {
                gTesting.Fatal("last UTID chk failed")
            }
		}
	}
}

type testSession struct {
	ski.SessionTool

	encoder pdi.TxnEncoder
	decoder pdi.TxnDecoder
}
