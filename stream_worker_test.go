package flo

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// oneRecordGroupRead is a GroupRead body carrying a single record.
func oneRecordGroupRead() []byte {
	b := make([]byte, 0, 64)
	b = binary.LittleEndian.AppendUint32(b, 1) // count
	b = binary.LittleEndian.AppendUint64(b, 1) // sequence
	b = binary.LittleEndian.AppendUint64(b, 1) // timestamp_ms
	b = append(b, 0)                           // tier
	b = binary.LittleEndian.AppendUint32(b, 0) // partition
	b = append(b, 0)                           // key_present
	b = binary.LittleEndian.AppendUint32(b, 1) // payload length
	b = append(b, 'x')                         // payload
	b = binary.LittleEndian.AppendUint32(b, 0) // header count
	return b
}

func writeFakeResponse(conn net.Conn, requestID uint64, data []byte) error {
	frame := make([]byte, HeaderSize+len(data))
	binary.LittleEndian.PutUint32(frame[0:4], Magic)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(data)))
	binary.LittleEndian.PutUint64(frame[8:16], requestID)
	frame[20] = Version
	frame[21] = byte(StatusOK)
	copy(frame[HeaderSize:], data)
	binary.LittleEndian.PutUint32(frame[16:20], computeCRC32(frame[:HeaderSize], data))
	_, err := conn.Write(frame)
	return err
}

// An ack must not queue behind the worker's next blocking GroupRead: that
// read can hold the polling connection for the whole BlockMS.
func TestStreamWorkerAckDoesNotWaitForPoll(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	const block = 3 * time.Second
	var reads atomic.Int32
	acked := make(chan time.Time, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					header := make([]byte, HeaderSize)
					if _, err := io.ReadFull(conn, header); err != nil {
						return
					}
					payload := make([]byte, binary.LittleEndian.Uint32(header[4:8]))
					if _, err := io.ReadFull(conn, payload); err != nil {
						return
					}
					id := binary.LittleEndian.Uint64(header[8:16])
					var data []byte
					switch OpCode(binary.LittleEndian.Uint16(header[20:22])) {
					case OpStreamGroupRead:
						if reads.Add(1) == 1 {
							data = oneRecordGroupRead()
						} else {
							time.Sleep(block) // nothing new: the server parks the read
						}
					case OpStreamGroupAck:
						select {
						case acked <- time.Now():
						default:
						}
					}
					if writeFakeResponse(conn, id, data) != nil {
						return
					}
				}
			}(conn)
		}
	}()

	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	delivered := make(chan time.Time, 1)
	handler := func(*StreamContext) error {
		delivered <- time.Now()
		// Let the poll loop issue its next blocking read first.
		time.Sleep(100 * time.Millisecond)
		return nil
	}
	sw, err := c.NewStreamWorker(StreamWorkerOptions{
		Stream:                      "s",
		BlockMS:                     uint32(block / time.Millisecond),
		RedeliverPendingOnReconnect: new(bool),
	}, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sw.Close()
	go sw.Start(context.Background())

	got := <-delivered
	select {
	case at := <-acked:
		if wait := at.Sub(got); wait > block/2 {
			t.Fatalf("ack arrived %v after delivery, behind the blocking read", wait)
		}
	case <-time.After(2 * block):
		t.Fatal("no ack")
	}
}
