package ski // import "github.com/plan-tools/go-plan/ski"

import (
	crypto_rand "crypto/rand"
)

var salts = saltGenerator()

type salt [24]byte

// needed by the SKI to cast our typedef
func saltToArray(n salt) *[24]byte {
	var arr [24]byte
	copy(n[:], arr[:24])
	return &arr
}

func saltGenerator() <-chan [24]byte {
	saltChan := make(chan [24]byte) // note: *must* be a blocking chan!
	go func() {
		for {
			currentSalt := make([]byte, 24)
			_, err := crypto_rand.Read(currentSalt)
			if err != nil {
				panic(err) // TODO: unclear when we'd ever hit this?
			}
			var salt salt
			copy(salt[:24], currentSalt[:])
			saltChan <- salt
		}
	}()
	return saltChan
}
