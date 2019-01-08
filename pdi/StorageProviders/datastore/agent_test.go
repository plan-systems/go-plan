package datastore

import (
	"bytes"
    "testing"
    "math/rand"
    //"io/ioutil"
    //"os"

    "github.com/plan-systems/go-plan/pdi"
    "github.com/plan-systems/go-plan/plan"
)


var gTestBuf = "May PLAN empower organizations and individuals, and may it be an instrument of productivity and self-organization."

var gTesting *testing.T

func TestAgent(t *testing.T) {

    gTesting = t

	seed := plan.Now().UnixSecs
	gTesting.Logf("Using seed: %d", seed)
	rand.Seed(seed)

 
    testBufs := make([][]byte, 100)
    
    for i := range(testBufs) {
    	N := int(1 + rand.Int31n(int32(len(gTestBuf))-2))
        testBufs[i] = []byte(gTestBuf[:N])
    }

    buf := make([]byte, len(testBufs) * len(gTestBuf))
    totalLen := 0

    var err error
    for i := range(testBufs) {
        totalLen, err = pdi.AppendVarBuf(buf, totalLen, testBufs[i])
        if err != nil {
            t.Fatal(err)
        }
    }
    buf = buf[:totalLen]

    offset := 0
    var payload []byte

    for i := range(testBufs) {
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



