package flo

import (
	"encoding/binary"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// refusalBody encodes a refusal's body as the server does.
func refusalBody(reason Reason, ran Ran, msg string) []byte {
	b := make([]byte, 3+len(msg))
	binary.LittleEndian.PutUint16(b[0:2], uint16(reason))
	b[2] = byte(ran)
	copy(b[3:], msg)
	return b
}

// statusReply builds a response frame for requestID with status and a
// refusal body carrying msg.
func statusReply(requestID uint64, status StatusCode, msg string) []byte {
	body := refusalBody(ReasonUnclassified, RanNo, msg)
	frame := make([]byte, HeaderSize+len(body))
	binary.LittleEndian.PutUint32(frame[0:4], Magic)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(body)))
	binary.LittleEndian.PutUint64(frame[8:16], requestID)
	frame[20] = Version
	binary.LittleEndian.PutUint64(frame[24:32], TableHash)
	frame[21] = byte(status)
	copy(frame[HeaderSize:], body)
	binary.LittleEndian.PutUint32(frame[16:20], computeCRC32(frame[:HeaderSize], body))
	return frame
}

func TestStatusUnavailableMapsToRetryableError(t *testing.T) {
	err := checkStatus(StatusUnavailable, refusalBody(ReasonUnclassified, RanNo, "shard 3 is offline"), false)
	if !IsUnavailable(err) || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
	var se *ServerError
	if !errors.As(err, &se) || se.Message != "shard 3 is offline" {
		t.Fatalf("got %#v, want the server's message kept", err)
	}
	if IsInternal(err) || IsOverloaded(err) {
		t.Errorf("unavailable also matched another status: %v", err)
	}
}

// internal_error can mean "committed but not applied": it must never be
// classed with the retryable statuses.
func TestStatusInternalErrorIsNotRetryable(t *testing.T) {
	err := checkStatus(StatusInternalError, refusalBody(ReasonCommittedNotApplied, RanYes, "committed, not applied"), false)
	if !IsInternal(err) {
		t.Fatalf("got %v, want ErrInternal", err)
	}
	if IsUnavailable(err) || IsOverloaded(err) || IsConnectionError(err) {
		t.Errorf("internal_error classed as retryable: %v", err)
	}
}

func TestUnknownStatusMapsToServerErrorNamingIt(t *testing.T) {
	err := checkStatus(StatusCode(200), refusalBody(ReasonUnclassified, RanUnknown, "future thing"), false)
	var se *ServerError
	if !errors.As(err, &se) || se.Status != 200 || se.Message != "future thing" {
		t.Fatalf("got %#v, want ServerError{200, future thing}", err)
	}
	if !strings.Contains(err.Error(), "200") {
		t.Errorf("error %q does not name status 200", err)
	}
}

// A non-ok reply, known or not, is read whole: the next call on the same
// connection gets its own reply, not the leftover body.
func TestErrorStatusDoesNotDesyncNextCall(t *testing.T) {
	for _, status := range []StatusCode{StatusUnavailable, StatusCode(200)} {
		t.Run(status.String(), func(t *testing.T) {
			var n atomic.Int32
			ln := serveAll(t, func(id uint64) []byte {
				if n.Add(1) == 1 {
					return statusReply(id, status, "not now")
				}
				return fakeReply(id, "fresh")
			})
			c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
			if err := c.Connect(); err != nil {
				t.Fatal(err)
			}
			defer c.Close()

			_, err := c.KV.Get("k", nil)
			var se *ServerError
			if !errors.As(err, &se) || se.Status != status || se.Message != "not now" {
				t.Fatalf("first Get: got %v, want status %d with the server's message", err, status)
			}
			if !c.IsConnected() {
				t.Fatal("a whole error reply dropped the connection")
			}
			res, err := c.KV.Get("k", nil)
			if err != nil || string(res.Value) != "fresh" {
				t.Fatalf("second Get: got %v, %v; want fresh", res, err)
			}
		})
	}
}

// An answer from a server built from another table is refused before its
// body is read, naming both tables, and the connection is dropped so the
// unread body can't be taken for the next call's answer.
func TestAnswerFromAnotherTableIsRefusedUnread(t *testing.T) {
	ln := serveAll(t, func(id uint64) []byte {
		frame := fakeReply(id, "from elsewhere")
		binary.LittleEndian.PutUint64(frame[24:32], TableHash+1)
		binary.LittleEndian.PutUint32(frame[16:20], computeCRC32(frame[:HeaderSize], frame[HeaderSize:]))
		return frame
	})
	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	_, err := c.KV.Get("k", nil)
	var tm *TableMismatchError
	if !errors.As(err, &tm) || !errors.Is(err, ErrTableMismatch) || tm.ServerTable != TableHash+1 {
		t.Fatalf("got %v, want a TableMismatchError naming the server's table", err)
	}
	if !strings.Contains(err.Error(), "upgrade the client") {
		t.Errorf("error %q does not say what to do", err)
	}
	if c.IsConnected() {
		t.Error("the connection was kept with an unread body on it")
	}
}

func TestRequestsCarryTheTableHash(t *testing.T) {
	data, err := serializeRequest(1, OpKVGet, []byte("default"), []byte("k"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if data[22] != 2 || binary.LittleEndian.Uint64(data[24:32]) != 0x2827f6f0631754fe {
		t.Fatalf("header version %d table 0x%016x, want 2 and the pinned table", data[22], binary.LittleEndian.Uint64(data[24:32]))
	}
}

// A refusal's reason and ran reach the caller, and the reason shows in the
// text when the server gave one.
func TestRefusalCarriesReasonAndRan(t *testing.T) {
	err := checkStatus(StatusInternalError, refusalBody(ReasonCommittedNotApplied, RanYes, "write committed but not applied on this node — do not resend"), false)
	var se *ServerError
	if !errors.As(err, &se) || se.Reason != ReasonCommittedNotApplied || se.Ran != RanYes {
		t.Fatalf("got %#v, want committed_not_applied with ran yes", err)
	}
	if !strings.Contains(err.Error(), "/committed_not_applied)") {
		t.Errorf("error %q does not name its reason", err)
	}
	unclassified := checkStatus(StatusBadRequest, refusalBody(ReasonUnclassified, RanNo, "x"), false)
	if strings.Contains(unclassified.Error(), "unclassified") {
		t.Errorf("error %q names an unclassified reason", unclassified)
	}
	for _, body := range [][]byte{{1, 0}, {0xff, 0xff, 0}, {1, 0, 9}} {
		if err := checkStatus(StatusBadRequest, body, false); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("body %v: got %v, want a malformed refusal", body, err)
		}
	}
}
