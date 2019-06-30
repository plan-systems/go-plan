package ski

import (
	//"bytes"
    "math/rand"
    //"fmt"
    //"time"
    //"ioutil"

    "testing"

	"github.com/plan-systems/plan-core/plan"

)


var gTesting *testing.T
var gCommunityID = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

func TestKeyTome(t *testing.T) {

    gTesting = t

    seed := plan.Now()
    seed = int64(1550730342)
    gTesting.Logf("using seed %d", seed)

    // Test different sizes to catch sort edge cases
    testKeySizes := []int{
        2, 4, 3, 7, 12, 15, 16, 29, 32,
    }

    testEntries := make([]*KeyEntry, testKeySizes[len(testKeySizes)-1])


    for i := range testEntries {
        entry := &KeyEntry{
            KeyInfo: &KeyInfo{
                KeyType:     123,
                CryptoKit:   1234,
                TimeCreated: int64(1000 + i),
                PubKey:       make([]byte, 32),
            },
            PrivKey: make([]byte, 32),
        }

        for j := range entry.PrivKey {
            entry.PrivKey[j] = byte(i)
            entry.KeyInfo.PubKey[j] = byte(i)
        }

        testEntries[i] = entry
    }



    for _, numTestKeys := range testKeySizes {
        keyTomeTest(testEntries[:numTestKeys])
    }
}


func keyTomeTest(testKeys []*KeyEntry) {

    N := len(testKeys)

    testKeyring := Keyring{
        Name: []byte("testKeyring"),
        Keys: make([]*KeyEntry, N),
    }
    copy(testKeyring.Keys, testKeys)

    for j := 0; j < 1000; j++ {
        
        rand.Shuffle(N, func(i, j int) {
            testKeyring.Keys[i], testKeyring.Keys[j] = testKeyring.Keys[j], testKeyring.Keys[i]
        })
        testKeyring.SortedByPubKey = false

        for i := 0; i < N; i++ {
            if testKeyring.FetchKeyWithPrefix(testKeys[i].KeyInfo.PubKey[:8]) != testKeys[i] {
                gTesting.Fatal("failed basic fetch test")
            }
        }

        testKeyring.Optimize()

        for i := 1; i < N; i++ {
            if testKeyring.Keys[i-1].KeyInfo.PubKey[0] > testKeyring.Keys[i].KeyInfo.PubKey[0] {
                gTesting.Fatal("failed basic Keyring sort test")
            }
        }

        if testKeyring.FetchNewestKey().KeyInfo.PubKey[0] != byte(N-1) {
            gTesting.Fatal("failed basic Keyring newest test")
        }

        for i := 0; i < N; i++ {
            if testKeyring.FetchKeyWithPrefix(testKeys[i].KeyInfo.PubKey[:8]) != testKeys[i] {
                gTesting.Fatal("failed basic fetch test")
            }
        }
    }

    numAdds := int(100 * N)
    keysToAdd := make([]*KeyEntry, numAdds)
    krSrc := Keyring{
        Name: []byte("merge src"),
    }

    missing := [][]byte{
        []byte{0, 0, 1},
        []byte{0, 1},
        []byte{5, 4},
        []byte{255},
        []byte{255, 0},
    }

    
    // Check that MergeKeys works as expected -- exact dupes are dropped.
    // Add a ton of dupes to a Keyring and check that they all went poof
    //keyTome := KeyTome{}
    for j := 0; j < 500; j++ {

        for i := 0; i < numAdds; i++ {
            keysToAdd[i] = testKeys[i % N]
        }
        
        rand.Shuffle(numAdds, func(i, j int) {
            keysToAdd[i], keysToAdd[j] = keysToAdd[j], keysToAdd[i]
        })

        kr := Keyring{
            Name: []byte("merge dst"),
        }

        {
            pos := 0
            for pos < numAdds {
                end := pos + int(rand.Int31n(int32(N)))
                if end > numAdds {
                    end = numAdds
                }
                krSrc.Keys = keysToAdd[pos:end]
                krSrc.SortedByPubKey = false

                for k := range missing{
                    if kr.FetchKeyWithPrefix(missing[k]) != nil {
                        gTesting.Fatal("Keyring returned key for bad lookup")
                    }
                    if kr.FetchKey(missing[k]) != nil {
                        gTesting.Fatal("Keyring returned key for bad lookup")
                    }
                }

                kr.MergeKeys(&krSrc)
                if len(krSrc.Keys) > 0 {
                    gTesting.Fatal("Keyring.MergeKeys() returned problems -- dupes should have been dropped")
                }

                pos = end
            }
        }

        check(&kr, testKeys, "dupe test")
    }
}


func check(kr *Keyring, testKeys []*KeyEntry, testDesc string) {

    if len(kr.Keys) != len(testKeys) {
        gTesting.Fatal(testDesc + "-- missing keys")
    }
    
    for i := range testKeys {
        if kr.FetchKeyWithPrefix(testKeys[i].KeyInfo.PubKey[:8]) != testKeys[i] {
            gTesting.Fatal(testDesc)
        }
    }
}








