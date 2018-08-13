package ski // import "github.com/plan-tools/go-plan/ski"

import (
	crypto_rand "crypto/rand"
)

var salts = saltGenerator()


func saltGenerator() <-chan [24]byte {
	saltChan := make(chan [24]byte) // note: *must* be a blocking chan!
	go func() {
		for {
            var salt [24]byte
			_, err := crypto_rand.Read(salt[:])
			if err != nil {
				panic(err) // TODO: unclear when we'd ever hit this?
			}
			saltChan <- salt
		}
	}()
	return saltChan
}
