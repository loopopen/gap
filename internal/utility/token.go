package utility

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
)

func GenFastToken(length int) string {
	b := make([]byte, length)
	i := 0
	for ; i <= length-4; i += 4 {
		binary.LittleEndian.PutUint32(b[i:], rand.Uint32())
	}
	if i < length {
		val := rand.Uint32()
		for ; i < length; i++ {
			b[i] = byte(val)
			val >>= 8
		}
	}
	return hex.EncodeToString(b)
}
