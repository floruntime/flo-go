package flo

import (
	"encoding/binary"
	"hash/crc32"
)

// OptionsBuilder builds TLV-encoded options.
type OptionsBuilder struct {
	buf []byte
}

// NewOptionsBuilder creates a new OptionsBuilder.
func NewOptionsBuilder() *OptionsBuilder {
	return &OptionsBuilder{buf: make([]byte, 0, 64)}
}

// AddU8 adds a u8 option.
func (b *OptionsBuilder) AddU8(tag OptionTag, value uint8) *OptionsBuilder {
	b.buf = append(b.buf, byte(tag), 1, value)
	return b
}

// AddU32 adds a u32 option.
func (b *OptionsBuilder) AddU32(tag OptionTag, value uint32) *OptionsBuilder {
	b.buf = append(b.buf, byte(tag), 4)
	b.buf = binary.LittleEndian.AppendUint32(b.buf, value)
	return b
}

// AddU64 adds a u64 option.
func (b *OptionsBuilder) AddU64(tag OptionTag, value uint64) *OptionsBuilder {
	b.buf = append(b.buf, byte(tag), 8)
	b.buf = binary.LittleEndian.AppendUint64(b.buf, value)
	return b
}

// AddBytes adds a bytes option.
func (b *OptionsBuilder) AddBytes(tag OptionTag, value []byte) *OptionsBuilder {
	if len(value) > 255 {
		panic("flo: option value too large (max 255 bytes)")
	}
	b.buf = append(b.buf, byte(tag), byte(len(value)))
	b.buf = append(b.buf, value...)
	return b
}

// AddFlag adds a flag option (presence indicates true).
func (b *OptionsBuilder) AddFlag(tag OptionTag) *OptionsBuilder {
	b.buf = append(b.buf, byte(tag), 0)
	return b
}

// Build returns the built options as bytes.
func (b *OptionsBuilder) Build() []byte {
	return b.buf
}

// extractBlockMS scans TLV-encoded options for OptBlockMS (0x17) and returns
// its value in milliseconds. Returns 0 if not found.
func extractBlockMS(options []byte) uint32 {
	for i := 0; i+1 < len(options); {
		tag := options[i]
		length := int(options[i+1])
		i += 2
		if tag == byte(OptBlockMS) && length == 4 && i+4 <= len(options) {
			return binary.LittleEndian.Uint32(options[i : i+4])
		}
		i += length
	}
	return 0
}

// computeCRC32 computes CRC32 for header (excluding crc32 field) + payload.
func computeCRC32(header, payload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(header[0:16])  // magic, payload_length, request_id
	h.Write(header[20:32]) // op_code/version/status, flags, reserved
	h.Write(payload)
	return h.Sum32()
}

// serializeRequest serializes a request into wire format.
func serializeRequest(requestID uint64, opCode OpCode, namespace, key, value, options []byte) ([]byte, error) {
	// Validate sizes
	if len(namespace) > MaxNamespaceSize {
		return nil, ErrNamespaceTooLarge
	}
	if len(key) > MaxKeySize {
		return nil, ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return nil, ErrValueTooLarge
	}

	// Calculate payload size
	payloadLen := 2 + len(namespace) + // namespace: [len:u16][data]
		2 + len(key) + // key: [len:u16][data]
		4 + len(value) + // value: [len:u32][data]
		2 + len(options) // options: [len:u16][data]

	// Allocate buffer for header + payload
	buf := make([]byte, HeaderSize+payloadLen)

	// Build header (without CRC32)
	binary.LittleEndian.PutUint32(buf[0:4], Magic)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(payloadLen))
	binary.LittleEndian.PutUint64(buf[8:16], requestID)
	// CRC32 at [16:20] - filled later
	binary.LittleEndian.PutUint16(buf[20:22], uint16(opCode))
	buf[22] = Version
	buf[23] = 0 // flags
	// bytes 24-31 are reserved (already zero)

	// Build payload
	offset := HeaderSize

	// Namespace
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(namespace)))
	offset += 2
	copy(buf[offset:], namespace)
	offset += len(namespace)

	// Key
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(key)))
	offset += 2
	copy(buf[offset:], key)
	offset += len(key)

	// Value
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(value)))
	offset += 4
	copy(buf[offset:], value)
	offset += len(value)

	// Options
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(options)))
	offset += 2
	copy(buf[offset:], options)

	// Compute and fill CRC32
	crc := computeCRC32(buf[:HeaderSize], buf[HeaderSize:])
	binary.LittleEndian.PutUint32(buf[16:20], crc)

	return buf, nil
}

// rawResponse represents a raw response from the server.
type rawResponse struct {
	Status    StatusCode
	Data      []byte
	RequestID uint64
}

// parseResponseHeader parses a response header.
// Returns (status, dataLen, requestID, crc, error).
func parseResponseHeader(header []byte) (StatusCode, uint32, uint64, uint32, error) {
	if len(header) < HeaderSize {
		return 0, 0, 0, 0, ErrIncompleteResponse
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != Magic {
		return 0, 0, 0, 0, ErrInvalidMagic
	}

	dataLen := binary.LittleEndian.Uint32(header[4:8])
	requestID := binary.LittleEndian.Uint64(header[8:16])
	crc := binary.LittleEndian.Uint32(header[16:20])
	version := header[20]
	status := StatusCode(header[21])

	if version != Version {
		return 0, 0, 0, 0, ErrUnsupportedVersion
	}

	return status, dataLen, requestID, crc, nil
}

// parseScanResponse parses scan response data.
func parseScanResponse(data []byte) (*ScanResult, error) {
	if len(data) < 9 {
		return nil, ErrIncompleteResponse
	}

	offset := 0

	// has_more
	hasMore := data[offset] != 0
	offset++

	// cursor
	cursorLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	if len(data) < offset+int(cursorLen) {
		return nil, ErrIncompleteResponse
	}

	var cursor []byte
	if cursorLen > 0 {
		cursor = make([]byte, cursorLen)
		copy(cursor, data[offset:offset+int(cursorLen)])
	}
	offset += int(cursorLen)

	// count
	if len(data) < offset+4 {
		return nil, ErrIncompleteResponse
	}

	count := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// entries
	entries := make([]KVEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		// key
		if len(data) < offset+2 {
			return nil, ErrIncompleteResponse
		}

		keyLen := binary.LittleEndian.Uint16(data[offset:])
		offset += 2

		if len(data) < offset+int(keyLen) {
			return nil, ErrIncompleteResponse
		}

		key := make([]byte, keyLen)
		copy(key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		// value
		if len(data) < offset+4 {
			return nil, ErrIncompleteResponse
		}

		valueLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		var value []byte
		if valueLen > 0 {
			if len(data) < offset+int(valueLen) {
				return nil, ErrIncompleteResponse
			}
			value = make([]byte, valueLen)
			copy(value, data[offset:offset+int(valueLen)])
			offset += int(valueLen)
		}

		entries = append(entries, KVEntry{Key: key, Value: value})
	}

	return &ScanResult{
		Entries: entries,
		Cursor:  cursor,
		HasMore: hasMore,
	}, nil
}

// parseHistoryResponse parses history response data.
func parseHistoryResponse(data []byte) ([]VersionEntry, error) {
	if len(data) < 4 {
		return nil, ErrIncompleteResponse
	}

	offset := 0

	count := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	entries := make([]VersionEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(data) < offset+20 {
			return nil, ErrIncompleteResponse
		}

		version := binary.LittleEndian.Uint64(data[offset:])
		offset += 8

		timestamp := int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		valueLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if len(data) < offset+int(valueLen) {
			return nil, ErrIncompleteResponse
		}

		value := make([]byte, valueLen)
		copy(value, data[offset:offset+int(valueLen)])
		offset += int(valueLen)

		entries = append(entries, VersionEntry{
			Version:   version,
			Timestamp: timestamp,
			Value:     value,
		})
	}

	return entries, nil
}

// parseDequeueResponse parses dequeue response data.
func parseDequeueResponse(data []byte) (*DequeueResult, error) {
	if len(data) < 4 {
		return nil, ErrIncompleteResponse
	}

	offset := 0

	count := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	messages := make([]Message, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(data) < offset+12 {
			return nil, ErrIncompleteResponse
		}

		seq := binary.LittleEndian.Uint64(data[offset:])
		offset += 8

		payloadLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if len(data) < offset+int(payloadLen) {
			return nil, ErrIncompleteResponse
		}

		payload := make([]byte, payloadLen)
		copy(payload, data[offset:offset+int(payloadLen)])
		offset += int(payloadLen)

		messages = append(messages, Message{
			Seq:     seq,
			Payload: payload,
		})
	}

	return &DequeueResult{Messages: messages}, nil
}

// parseEnqueueResponse parses enqueue response data.
func parseEnqueueResponse(data []byte) (uint64, error) {
	if len(data) < 8 {
		return 0, ErrIncompleteResponse
	}
	return binary.LittleEndian.Uint64(data[:8]), nil
}

// serializeSeqs serializes sequence numbers for ack/nack.
func serializeSeqs(seqs []uint64) []byte {
	buf := make([]byte, 4+len(seqs)*8)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(seqs)))
	offset := 4
	for _, seq := range seqs {
		binary.LittleEndian.PutUint64(buf[offset:], seq)
		offset += 8
	}
	return buf
}
