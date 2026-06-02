package transport

import (
	"encoding/binary"
)

func EncodeTLV(tag byte, value []byte) []byte {
	buf := make([]byte, 3+len(value))

	buf[0] = tag
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(value)))
	copy(buf[3:], value)

	return buf
}

func DecodeTLV(data []byte) map[byte][]byte {
	out := make(map[byte][]byte)

	i := 0
	for i < len(data) {
		tag := data[i]
		length := binary.BigEndian.Uint16(data[i+1 : i+3])

		value := make([]byte, length)
		copy(value, data[i+3:i+3+int(length)])

		out[tag] = value
		i += 3 + int(length)
	}

	return out
}