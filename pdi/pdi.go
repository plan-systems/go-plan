// See pdi.proto

package pdi

import (
	"github.com/ethereum/go-ethereum/crypto/sha3"
)

// EntryVersionMask is a bit mask on EntryCrypt.CryptInfo to extract pdi.EntryVersion
const EntryVersionMask = 0xFF

// GetEntryVersion returns the version of this entry (should match EntryVersion1)
func (entry *EntryCrypt) GetEntryVersion() EntryVersion {
	return EntryVersion(entry.CryptInfo & EntryVersionMask)
}

// ComputeHash hashes all fields of psi.EntryCrypt (except .EntrySig)
func (entry *EntryCrypt) ComputeHash() []byte {

	hw := sha3.NewKeccak256()

	scrap := make([]byte, 16)

	pos := 0
	pos = encodeVarintPdi(scrap, pos, entry.CryptInfo)

	hw.Write(scrap[:pos])
	hw.Write(entry.CommunityKeyId[:])
	hw.Write(entry.HeaderCrypt)
	hw.Write(entry.BodyCrypt)

	return hw.Sum(nil)

}
