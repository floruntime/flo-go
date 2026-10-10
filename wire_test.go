package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
		b.AddU64(OptTTLMs, 3_600_000)
		result := b.Build()

		expected := make([]byte, 10)
		expected[0] = byte(OptTTLMs)
		expected[1] = 8
		binary.LittleEndian.PutUint64(expected[2:], 3_600_000)

		if !bytes.Equal(result, expected) {
			t.Errorf("expected %v, got %v", expected, result)
		}
	})

	t.Run("AddBytes", func(t *testing.T) {
		b := NewOptionsBuilder()
		b.AddBytes(OptRoutingKey, []byte("test-key"))
		result := b.Build()

		expected := append([]byte{byte(OptRoutingKey), 8}, []byte("test-key")...)

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
			AddU64(OptTTLMs, 1000).
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
		if data[22] != Version {
			t.Errorf("expected version %d, got %d", Version, data[22])
		}

		// Check opcode (u16 LE at bytes 20-21)
		opcode := binary.LittleEndian.Uint16(data[20:22])
		if opcode != uint16(OpKVGet) {
			t.Errorf("expected opcode 0x%04X, got 0x%04X", OpKVGet, opcode)
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
		binary.LittleEndian.PutUint64(header[24:32], TableHash)
		header[21] = byte(StatusOK)
		header[22] = 0 // flags
		header[23] = 0 // pad

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
		binary.LittleEndian.PutUint64(header[24:32], TableHash)

		_, _, _, _, err := parseResponseHeader(header)
		if err != ErrInvalidMagic {
			t.Errorf("expected ErrInvalidMagic, got %v", err)
		}
	})

	t.Run("AnotherProtocolOrTable", func(t *testing.T) {
		for _, c := range []struct {
			version uint8
			table   uint64
			want    string
		}{
			{1, 0, "flo: server protocol 1, client protocol 2: upgrade the client"},
			{Version, TableHash + 1, fmt.Sprintf("flo: server table 0x%016x, client table 0x%016x: upgrade the client", TableHash+1, TableHash)},
		} {
			header := make([]byte, HeaderSize)
			binary.LittleEndian.PutUint32(header[0:4], Magic)
			header[20] = c.version
			binary.LittleEndian.PutUint64(header[24:32], c.table)
			_, _, _, _, err := parseResponseHeader(header)
			if !errors.Is(err, ErrTableMismatch) || err.Error() != c.want {
				t.Errorf("got %v, want %q", err, c.want)
			}
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
		// count(4) + has_more(1) + cursor_len(2)
		data := make([]byte, 7)

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

		// has_more = true, cursor = "cur1"
		buf = append(buf, 1)
		buf = binary.LittleEndian.AppendUint16(buf, 4)
		buf = append(buf, []byte("cur1")...)

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
		// Each message as the server writes it:
		// [seq:u64][payload_len:u32][payload][enqueued_at_ms:i64][delivery_count:u32][priority:u8]
		want := []Message{
			{Seq: 100, Payload: []byte("msg1"), EnqueuedAtMS: 1700000000000, DeliveryCount: 1, Priority: 0},
			{Seq: 101, Payload: []byte("m2"), EnqueuedAtMS: 1700000000005, DeliveryCount: 2, Priority: 7},
			{Seq: 102, Payload: []byte("third"), EnqueuedAtMS: 1700000000009, DeliveryCount: 3, Priority: 255},
		}
		buf := binary.LittleEndian.AppendUint32(nil, uint32(len(want)))
		for _, m := range want {
			buf = binary.LittleEndian.AppendUint64(buf, m.Seq)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.Payload)))
			buf = append(buf, m.Payload...)
			buf = binary.LittleEndian.AppendUint64(buf, uint64(m.EnqueuedAtMS))
			buf = binary.LittleEndian.AppendUint32(buf, m.DeliveryCount)
			buf = append(buf, m.Priority)
		}

		result, err := parseDequeueResponse(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result.Messages) != len(want) {
			t.Fatalf("expected %d messages, got %d", len(want), len(result.Messages))
		}
		for i, m := range want {
			got := result.Messages[i]
			if got.Seq != m.Seq || !bytes.Equal(got.Payload, m.Payload) || got.EnqueuedAtMS != m.EnqueuedAtMS || got.DeliveryCount != m.DeliveryCount || got.Priority != m.Priority {
				t.Errorf("message %d: got %+v, want %+v", i, got, m)
			}
		}
	})

	t.Run("RefusesACountTheDataCantHold", func(t *testing.T) {
		buf := binary.LittleEndian.AppendUint32(nil, 0xFFFFFFFF)
		if _, err := parseDequeueResponse(buf); err != ErrIncompleteResponse {
			t.Fatalf("expected ErrIncompleteResponse, got %v", err)
		}
	})

	t.Run("RefusesACutTrailer", func(t *testing.T) {
		buf := binary.LittleEndian.AppendUint32(nil, 1)
		buf = binary.LittleEndian.AppendUint64(buf, 1)
		buf = binary.LittleEndian.AppendUint32(buf, 1)
		buf = append(buf, 'x')
		buf = binary.LittleEndian.AppendUint64(buf, 0)
		buf = append(buf, 0, 0, 0, 0)
		if _, err := parseDequeueResponse(buf); err != ErrIncompleteResponse {
			t.Fatalf("expected ErrIncompleteResponse, got %v", err)
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
	binary.LittleEndian.PutUint16(header[20:22], uint16(OpKVGet))
	header[22] = Version

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

func TestExtractBlockMS(t *testing.T) {
	t.Run("Present", func(t *testing.T) {
		opts := NewOptionsBuilder().
			AddU8(OptPriority, 5).
			AddU32(OptBlockMS, 30000).
			AddFlag(OptIfNotExists).
			Build()

		if got := extractBlockMS(opts); got != 30000 {
			t.Errorf("expected 30000, got %d", got)
		}
	})

	t.Run("Absent", func(t *testing.T) {
		opts := NewOptionsBuilder().
			AddU8(OptPriority, 5).
			Build()

		if got := extractBlockMS(opts); got != 0 {
			t.Errorf("expected 0, got %d", got)
		}
	})

	t.Run("Nil", func(t *testing.T) {
		if got := extractBlockMS(nil); got != 0 {
			t.Errorf("expected 0, got %d", got)
		}
	})

	t.Run("Empty", func(t *testing.T) {
		if got := extractBlockMS([]byte{}); got != 0 {
			t.Errorf("expected 0, got %d", got)
		}
	})
}

func TestEncodeInvokeValue(t *testing.T) {
	t.Run("no labels", func(t *testing.T) {
		got, err := encodeInvokeValue("", []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		if want := []byte{0, 'x'}; !bytes.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("labels", func(t *testing.T) {
		got, err := encodeInvokeValue(`{"gpu":true}`, []byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		want := append([]byte{1, 12, 0}, `{"gpu":true}x`...)
		if !bytes.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("labels too long", func(t *testing.T) {
		if _, err := encodeInvokeValue(string(make([]byte, 65536)), nil); err == nil {
			t.Error("expected an error for labels over 65535 bytes")
		}
	})
}

func TestParseActionInvokeResult(t *testing.T) {
	got, err := parseActionInvokeResult(append([]byte{3, 0}, "act\x00"...))
	if err != nil {
		t.Fatal(err)
	}
	if got.RunID != "act" {
		t.Errorf("expected run id %q, got %q", "act", got.RunID)
	}
	for _, bad := range [][]byte{nil, {5}, {0, 0}, {9, 0, 'a'}} {
		if _, err := parseActionInvokeResult(bad); err == nil {
			t.Errorf("expected an error for %v", bad)
		}
	}
}
