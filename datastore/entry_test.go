package datastore

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"testing"
)

func TestEntry_EncodeDecode(t *testing.T) {
	original := entry{
		key:   "k1",
		value: []byte("v1"),
		Type:  "string",
	}
	original.CalculateChecksum()

	encoded := original.Encode()

	var decoded entry
	decoded.Decode(encoded)

	if decoded.key != original.key {
		t.Errorf("expected key %q, got %q", original.key, decoded.key)
	}
	if !bytes.Equal(decoded.value, original.value) {
		t.Errorf("expected value %q, got %q", original.value, decoded.value)
	}
	if decoded.Type != original.Type {
		t.Errorf("expected type %q, got %q", original.Type, decoded.Type)
	}

	expectedChecksum := sha1.Sum(original.value)
	if !bytes.Equal(decoded.Checksum, expectedChecksum[:]) {
		t.Errorf("checksum mismatch")
	}
}

func TestEntry_DecodeFromReader(t *testing.T) {
	original := entry{
		key:   "k2",
		value: []byte("v2"),
		Type:  "int64",
	}
	original.CalculateChecksum()

	encoded := original.Encode()

	var decoded entry
	n, err := decoded.DecodeFromReader(bufio.NewReader(bytes.NewReader(encoded)))
	if err != nil {
		t.Fatalf("DecodeFromReader error: %v", err)
	}
	if n != len(encoded) {
		t.Errorf("expected to read %d bytes, got %d", len(encoded), n)
	}
	if decoded.key != original.key {
		t.Errorf("expected key %q, got %q", original.key, decoded.key)
	}
	if !bytes.Equal(decoded.value, original.value) {
		t.Errorf("expected value %q, got %q", original.value, decoded.value)
	}
	if decoded.Type != original.Type {
		t.Errorf("expected type %q, got %q", original.Type, decoded.Type)
	}
	expectedChecksum := sha1.Sum(original.value)
	if !bytes.Equal(decoded.Checksum, expectedChecksum[:]) {
		t.Errorf("checksum mismatch")
	}
}
