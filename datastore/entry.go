package datastore

import (
	"bufio"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type entry struct {
	key      string
	value    []byte
	Type     string
	Checksum []byte
}

// 0           4    8     kl+8  kl+12     <-- offset
// (full size) (kl) (key) (vl)  (value)
// 4           4    ....  4     .....     <-- length

func (e *entry) Encode() []byte {
	kl, vl, tl := len(e.key), len(e.value), len(e.Type)
	size := 4 + 4 + kl + 4 + vl + 4 + tl + len(e.Checksum)

	res := make([]byte, size)
	offset := 0

	binary.LittleEndian.PutUint32(res[offset:], uint32(size))
	offset += 4

	binary.LittleEndian.PutUint32(res[offset:], uint32(kl))
	offset += 4
	copy(res[offset:], e.key)
	offset += kl

	binary.LittleEndian.PutUint32(res[offset:], uint32(vl))
	offset += 4
	copy(res[offset:], e.value)
	offset += vl

	binary.LittleEndian.PutUint32(res[offset:], uint32(tl))
	offset += 4
	copy(res[offset:], e.Type)
	offset += tl

	if len(e.Checksum) > 0 {
		copy(res[offset:], e.Checksum)
	}

	return res
}

func (e *entry) Decode(input []byte) {
	offset := 4

	kl := binary.LittleEndian.Uint32(input[offset:])
	offset += 4
	e.key = string(input[offset : offset+int(kl)])
	offset += int(kl)

	vl := binary.LittleEndian.Uint32(input[offset:])
	offset += 4
	e.value = make([]byte, vl)
	copy(e.value, input[offset:offset+int(vl)])
	offset += int(vl)

	tl := binary.LittleEndian.Uint32(input[offset:])
	offset += 4
	e.Type = string(input[offset : offset+int(tl)])
	offset += int(tl)

	if offset < len(input) {
		e.Checksum = make([]byte, sha1.Size)
		copy(e.Checksum, input[offset:])
	}
}

func decodeString(v []byte) string {
	l := binary.LittleEndian.Uint32(v)
	buf := make([]byte, l)
	copy(buf, v[4:4+int(l)])
	return string(buf)
}

func (e *entry) DecodeFromReader(in *bufio.Reader) (int, error) {
	sizeBuf, err := in.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return 0, fmt.Errorf("DecodeFromReader, cannot read size: %w", err)
	}
	size := int(binary.LittleEndian.Uint32(sizeBuf))
	buf := make([]byte, size)
	n, err := in.Read(buf[:])
	if err != nil {
		return n, fmt.Errorf("DecodeFromReader, cannot read record: %w", err)
	}
	e.Decode(buf)
	return n, nil
}

func (e *entry) CalculateChecksum() {
	if len(e.value) == 0 {
		e.Checksum = nil
		return
	}
	hash := sha1.Sum(e.value)
	e.Checksum = hash[:]
}
