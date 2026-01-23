package flo

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestOptionsBuilder(t *testing.T) {
	t.Run("AddU8", func(t *testing.T) {
		b := NewOptionsBuilder()
		b.AddU8(OptPriority, 42)
		result := b.Build()

		expected := []byte{byte(OptPriority), 1, 42}
		if !bytes.Equal(result, expected) {
			t.Errorf("expected %v, got %v", expected, result)
		}
	})

	t.Run("AddU32", func(t *testing.T) {
		b := NewOptionsBuilder()
		b.AddU32(OptLimit, 1000)
		result := b.Build()

		expected := make([]byte, 6)
		expected[0] = byte(OptLimit)
		expected[1] = 4
		binary.LittleEndian.PutUint32(expected[2:], 1000)

		if !bytes.Equal(result, expected) {
			t.Errorf("expected %v, got %v", expected, result)
		}
	})

	t.Run("AddU64", func(t *testing.T) {
		b := NewOptionsBuilder()
		b.AddU64(OptTTLSeconds, 3600)
		result := b.Build()

		expected := make([]byte, 10)
		expected[0] = byte(OptTTLSeconds)
		expected[1] = 8
		binary.LittleEndian.PutUint64(expected[2:], 3600)

		if !bytes.Equal(result, expected) {
			t.Errorf("expected %v, got %v", expected, result)
		}
	})

	t.Run("AddBytes", func(t *testing.T) {
		b := NewOptionsBuilder()
		b.AddBytes(OptDedupKey, []byte("test-key"))
		result := b.Build()

		expected := append([]byte{byte(OptDedupKey), 8}, []byte("test-key")...)

		if !bytes.Equal(result, expected) {
			t.Errorf("expected %v, got %v", expected, result)
		}
	})

	t.Run("AddFlag", func(t *testing.T) {
		b := NewOptionsBuilder()
		b.AddFlag(OptIfNotExists)
		result := b.Build()

		expected := []byte{byte(OptIfNotExists), 0}
		if !bytes.Equal(result, expected) {
			t.Errorf("expected %v, got %v", expected, result)
		}
	})

	t.Run("ChainedOptions", func(t *testing.T) {
		b := NewOptionsBuilder()
		b.AddU8(OptPriority, 5).
			AddU64(OptDelayMS, 1000).
			AddFlag(OptIfNotExists)

		result := b.Build()
		if len(result) != 3+10+2 { // u8(3) + u64(10) + flag(2)
			t.Errorf("expected length 15, got %d", len(result))
		}
	})
}

func TestSerializeRequest(t *testing.T) {
	t.Run("BasicRequest", func(t *testing.T) {
		data, err := serializeRequest(
			1,
			OpKVGet,
			[]byte("myns"),
			[]byte("mykey"),
			nil,
			nil,
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Check magic
		magic := binary.LittleEndian.Uint32(data[0:4])
		if magic != Magic {
			t.Errorf("expected magic 0x%08X, got 0x%08X", Magic, magic)
		}

		// Check version
		if data[20] != Version {
			t.Errorf("expected version %d, got %d", Version, data[20])
		}

		// Check opcode
		if data[21] != byte(OpKVGet) {
			t.Errorf("expected opcode 0x%02X, got 0x%02X", OpKVGet, data[21])
		}
	})

	t.Run("NamespaceTooLarge", func(t *testing.T) {
		namespace := make([]byte, MaxNamespaceSize+1)
		_, err := serializeRequest(1, OpKVGet, namespace, []byte("key"), nil, nil)
		if err != ErrNamespaceTooLarge {
			t.Errorf("expected ErrNamespaceTooLarge, got %v", err)
		}
	})

	t.Run("KeyTooLarge", func(t *testing.T) {
		key := make([]byte, MaxKeySize+1)
		_, err := serializeRequest(1, OpKVGet, []byte("ns"), key, nil, nil)
		if err != ErrKeyTooLarge {
			t.Errorf("expected ErrKeyTooLarge, got %v", err)
		}
	})

	t.Run("ValueTooLarge", func(t *testing.T) {
		value := make([]byte, MaxValueSize+1)
		_, err := serializeRequest(1, OpKVPut, []byte("ns"), []byte("key"), value, nil)
		if err != ErrValueTooLarge {
			t.Errorf("expected ErrValueTooLarge, got %v", err)
		}
	})
}

func TestParseResponseHeader(t *testing.T) {
	t.Run("ValidHeader", func(t *testing.T) {
		header := make([]byte, HeaderSize)
		binary.LittleEndian.PutUint32(header[0:4], Magic)
		binary.LittleEndian.PutUint32(header[4:8], 100) // dataLen
		binary.LittleEndian.PutUint64(header[8:16], 42) // requestID
		binary.LittleEndian.PutUint32(header[16:20], 0) // CRC (will be computed)
		header[20] = Version
		header[21] = byte(StatusOK)
		header[22] = 0 // flags
		header[23] = 0 // reserved

		status, dataLen, requestID, _, err := parseResponseHeader(header)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if status != StatusOK {
			t.Errorf("expected StatusOK, got %v", status)
		}
		if dataLen != 100 {
			t.Errorf("expected dataLen 100, got %d", dataLen)
		}
		if requestID != 42 {
			t.Errorf("expected requestID 42, got %d", requestID)
		}
	})

	t.Run("InvalidMagic", func(t *testing.T) {
		header := make([]byte, HeaderSize)
		binary.LittleEndian.PutUint32(header[0:4], 0xDEADBEEF)
		header[20] = Version

		_, _, _, _, err := parseResponseHeader(header)
		if err != ErrInvalidMagic {
			t.Errorf("expected ErrInvalidMagic, got %v", err)
		}
	})

	t.Run("UnsupportedVersion", func(t *testing.T) {
		header := make([]byte, HeaderSize)
		binary.LittleEndian.PutUint32(header[0:4], Magic)
		header[20] = 0xFF // bad version

		_, _, _, _, err := parseResponseHeader(header)
		if err != ErrUnsupportedVersion {
			t.Errorf("expected ErrUnsupportedVersion, got %v", err)
		}
	})

	t.Run("TooShort", func(t *testing.T) {
		header := make([]byte, 10) // too short

		_, _, _, _, err := parseResponseHeader(header)
		if err != ErrIncompleteResponse {
			t.Errorf("expected ErrIncompleteResponse, got %v", err)
		}
	})
}

func TestParseScanResponse(t *testing.T) {
	t.Run("EmptyResult", func(t *testing.T) {
		// has_more(1) + cursor_len(4) + cursor(0) + count(4)
		data := make([]byte, 9)
		data[0] = 0 // has_more = false
		// cursor_len = 0 (bytes 1-4)
		// count = 0 (bytes 5-8)

		result, err := parseScanResponse(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.HasMore {
			t.Error("expected HasMore to be false")
		}
		if result.Cursor != nil {
			t.Error("expected Cursor to be nil")
		}
		if len(result.Entries) != 0 {
			t.Errorf("expected 0 entries, got %d", len(result.Entries))
		}
	})

	t.Run("WithEntries", func(t *testing.T) {
		// Build response manually
		buf := make([]byte, 0, 100)

		// has_more = true
		buf = append(buf, 1)

		// cursor_len = 4
		buf = binary.LittleEndian.AppendUint32(buf, 4)
		buf = append(buf, []byte("cur1")...)

		// count = 2
		buf = binary.LittleEndian.AppendUint32(buf, 2)

		// Entry 1: key="key1", value="val1"
		buf = binary.LittleEndian.AppendUint16(buf, 4)
		buf = append(buf, []byte("key1")...)
		buf = binary.LittleEndian.AppendUint32(buf, 4)
		buf = append(buf, []byte("val1")...)

		// Entry 2: key="key2", value="val2"
		buf = binary.LittleEndian.AppendUint16(buf, 4)
		buf = append(buf, []byte("key2")...)
		buf = binary.LittleEndian.AppendUint32(buf, 4)
		buf = append(buf, []byte("val2")...)

		result, err := parseScanResponse(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if !result.HasMore {
			t.Error("expected HasMore to be true")
		}
		if !bytes.Equal(result.Cursor, []byte("cur1")) {
			t.Errorf("expected cursor 'cur1', got %s", result.Cursor)
		}
		if len(result.Entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(result.Entries))
		}
		if !bytes.Equal(result.Entries[0].Key, []byte("key1")) {
			t.Errorf("expected key 'key1', got %s", result.Entries[0].Key)
		}
		if !bytes.Equal(result.Entries[0].Value, []byte("val1")) {
			t.Errorf("expected value 'val1', got %s", result.Entries[0].Value)
		}
	})
}

func TestParseDequeueResponse(t *testing.T) {
	t.Run("EmptyResult", func(t *testing.T) {
		data := make([]byte, 4)
		// count = 0

		result, err := parseDequeueResponse(data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.Messages) != 0 {
			t.Errorf("expected 0 messages, got %d", len(result.Messages))
		}
	})

	t.Run("WithMessages", func(t *testing.T) {
		buf := make([]byte, 0, 100)

		// count = 2
		buf = binary.LittleEndian.AppendUint32(buf, 2)

		// Message 1: seq=100, payload="msg1"
		buf = binary.LittleEndian.AppendUint64(buf, 100)
		buf = binary.LittleEndian.AppendUint32(buf, 4)
		buf = append(buf, []byte("msg1")...)

		// Message 2: seq=101, payload="msg2"
		buf = binary.LittleEndian.AppendUint64(buf, 101)
		buf = binary.LittleEndian.AppendUint32(buf, 4)
		buf = append(buf, []byte("msg2")...)

		result, err := parseDequeueResponse(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(result.Messages) != 2 {
			t.Fatalf("expected 2 messages, got %d", len(result.Messages))
		}
		if result.Messages[0].Seq != 100 {
			t.Errorf("expected seq 100, got %d", result.Messages[0].Seq)
		}
		if !bytes.Equal(result.Messages[0].Payload, []byte("msg1")) {
			t.Errorf("expected payload 'msg1', got %s", result.Messages[0].Payload)
		}
	})
}

func TestSerializeSeqs(t *testing.T) {
	seqs := []uint64{100, 200, 300}
	result := serializeSeqs(seqs)

	// Check count
	count := binary.LittleEndian.Uint32(result[0:4])
	if count != 3 {
		t.Errorf("expected count 3, got %d", count)
	}

	// Check seqs
	offset := 4
	for i, expected := range seqs {
		actual := binary.LittleEndian.Uint64(result[offset:])
		if actual != expected {
			t.Errorf("seq[%d]: expected %d, got %d", i, expected, actual)
		}
		offset += 8
	}
}

func TestComputeCRC32(t *testing.T) {
	header := make([]byte, HeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], Magic)
	binary.LittleEndian.PutUint32(header[4:8], 5)
	binary.LittleEndian.PutUint64(header[8:16], 1)
	header[20] = Version
	header[21] = byte(OpKVGet)

	payload := []byte("hello")

	crc1 := computeCRC32(header, payload)
	crc2 := computeCRC32(header, payload)

	if crc1 != crc2 {
		t.Errorf("CRC32 should be deterministic: %d != %d", crc1, crc2)
	}

	// Modify payload and verify CRC changes
	payload2 := []byte("world")
	crc3 := computeCRC32(header, payload2)

	if crc1 == crc3 {
		t.Error("CRC32 should change with different payload")
	}
}
