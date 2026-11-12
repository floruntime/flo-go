package flo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// requestParts splits a value-and-options request's payload:
// [ns][key][value_len:u32][value][options_len:u16][options].
func requestParts(t *testing.T, p []byte) (key string, value, options []byte) {
	t.Helper()
	n := int(binary.LittleEndian.Uint16(p))
	off := 2 + n
	k := int(binary.LittleEndian.Uint16(p[off:]))
	key = string(p[off+2 : off+2+k])
	off += 2 + k
	v := int(binary.LittleEndian.Uint32(p[off:]))
	value = p[off+4 : off+4+v]
	off += 4 + v
	o := int(binary.LittleEndian.Uint16(p[off:]))
	return key, value, p[off+2 : off+2+o]
}

func TestGroupReadSendsGroupAndConsumerAndReadsTheLease(t *testing.T) {
	var value, options []byte
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		_, value, options = requestParts(t, p)
		return StatusOK, oneRecord
	})
	got, err := c.Stream.GroupRead("s", "g", "c1", &StreamGroupReadOptions{Count: Uint32Ptr(5)})
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{1, 0, 'g', 2, 0, 'c', '1'}; !bytes.Equal(value, want) {
		t.Errorf("read value % x, want % x", value, want)
	}
	if want := []byte{byte(OptCount), 4, 5, 0, 0, 0}; !bytes.Equal(options, want) {
		t.Errorf("read options % x, want % x", options, want)
	}
	if got.Lease != (StreamGroupLease{Consumer: "c1", Term: 2, Index: 7}) || len(got.Records) != 1 {
		t.Fatalf("read %+v", got)
	}
	r := got.Records[0]
	if r.ID != (StreamID{TimestampMS: 1, Sequence: 1}) || string(r.Payload) != "x" || r.Deliveries != 1 {
		t.Errorf("record %+v", r)
	}
}

func TestAnEmptyGroupReadLeasedNothing(t *testing.T) {
	got, err := parseGroupReadResponse(make([]byte, 28))
	if err != nil || len(got.Records) != 0 || got.ExpiresAtMS != 0 {
		t.Fatalf("empty read = %+v, %v", got, err)
	}
	for _, bad := range [][]byte{nil, oneRecord[:len(oneRecord)-1], append(append([]byte{}, oneRecord...), 0)} {
		if _, err := parseGroupReadResponse(bad); err == nil {
			t.Errorf("% x read without error", bad)
		}
	}
}

func TestGroupReportsNameTheDeliveryAndEachRecord(t *testing.T) {
	lease := StreamGroupLease{Consumer: "c", Term: 3, Index: 9}
	items := []StreamGroupItem{{ID: StreamID{TimestampMS: 5, Sequence: 6}, Deliveries: 2}}
	var value, options []byte
	var ops []OpCode
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		_, value, options = requestParts(t, p)
		ops = append(ops, op)
		if op == OpStreamGroupTouch {
			return StatusOK, append(binary.LittleEndian.AppendUint64(nil, 4242), groupReportBody1...)
		}
		return StatusOK, groupReportBody1
	})
	want := cat([]byte{1, 0, 'g', 1, 0, 'c'}, u64le(3), u64le(9), []byte{1, 0, 0, 0}, u64le(5), u64le(6), []byte{2, 0, 0, 0})
	if rep, err := c.Stream.GroupAck("s", "g", lease, items, nil); err != nil || rep.Applied != 1 || len(rep.Refused) != 0 {
		t.Fatalf("ack = %+v, %v", rep, err)
	}
	if !bytes.Equal(value, want) || len(options) != 0 {
		t.Errorf("ack value % x options % x, want % x and none", value, options, want)
	}
	if _, err := c.Stream.GroupNack("s", "g", lease, items, &StreamGroupNackOptions{DelayMS: 10, Dead: true, Error: "e"}); err != nil {
		t.Fatal(err)
	}
	wantOpts := cat([]byte{byte(OptDelayMS), 8}, u64le(10), []byte{byte(OptDead), 0, byte(OptErrorText), 1, 'e'})
	if !bytes.Equal(value, want) || !bytes.Equal(options, wantOpts) {
		t.Errorf("nack value % x options % x, want % x and % x", value, options, want, wantOpts)
	}
	rep, err := c.Stream.GroupTouch("s", "g", lease, items, nil)
	if err != nil || rep.ExpiresAtMS != 4242 || rep.Applied != 1 {
		t.Fatalf("touch = %+v, %v", rep, err)
	}
	if !bytes.Equal(value, want) {
		t.Errorf("touch value % x, want % x", value, want)
	}
	if want := []OpCode{OpStreamGroupAck, OpStreamGroupNack, OpStreamGroupTouch}; !equalOps(ops, want) {
		t.Errorf("ops %v, want %v", ops, want)
	}
}

func equalOps(a, b []OpCode) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A stale report is answered ok, applying nothing to the records whose
// delivery moved on, each with why. It is sent once: resending it can't
// apply it.
func TestAStaleGroupReportAppliesNothingAndSaysWhy(t *testing.T) {
	var sends atomic.Int32
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		sends.Add(1)
		body := binary.LittleEndian.AppendUint32(nil, 0) // applied
		body = binary.LittleEndian.AppendUint32(body, 1) // refused
		body = binary.LittleEndian.AppendUint64(body, 5)
		body = binary.LittleEndian.AppendUint64(body, 6)
		return StatusOK, append(body, byte(QueueDeliveredAgain))
	})
	rep, err := c.Stream.GroupAck("s", "g", StreamGroupLease{Term: 1, Index: 1}, []StreamGroupItem{{ID: StreamID{TimestampMS: 5, Sequence: 6}, Deliveries: 1}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Applied != 0 || len(rep.Refused) != 1 || rep.Refused[0].ID != (StreamID{TimestampMS: 5, Sequence: 6}) || rep.Refused[0].Why != QueueDeliveredAgain {
		t.Fatalf("report %+v, want one refused delivered_again", rep)
	}
	if n := sends.Load(); n != 1 {
		t.Errorf("sent %d times, want once", n)
	}
	if _, err := decodeGroupReport([]byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9}, false); err == nil {
		t.Error("a why past not_held was read")
	}
}

// The stream worker logs a stale ack and doesn't resend it.
func TestStreamWorkerLogsAStaleAckOnce(t *testing.T) {
	stale := cat([]byte{0, 0, 0, 0, 1, 0, 0, 0}, u64le(7), u64le(1), []byte{byte(QueueExpired)})
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op == OpStreamGroupAck {
			return reply{body: stale}
		}
		return reply{}
	})
	logger := &captureLogger{}
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{Stream: "s", Logger: logger}, func(*StreamContext) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	sw.reportWithRetry("s", testLease, testGroupRecord(), true, "")
	if n := len(s.got(OpStreamGroupAck)); n != 1 {
		t.Errorf("sent %d acks, want 1", n)
	}
	if !logger.has("applied nothing (expired)") {
		t.Errorf("no stale-ack line; log: %q", logger.all())
	}
}

func TestGroupRequestsOutOfBoundsAreRefusedBeforeARoundTrip(t *testing.T) {
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		t.Errorf("op 0x%x reached the server", op)
		return StatusOK, nil
	})
	lease := StreamGroupLease{}
	one := []StreamGroupItem{{}}
	refused := map[string]error{}
	_, refused["read without a group"] = c.Stream.GroupRead("s", "", "c", nil)
	_, refused["read group over"] = c.Stream.GroupRead("s", strings.Repeat("g", MaxGroupNameSize+1), "c", nil)
	_, refused["ack of none"] = c.Stream.GroupAck("s", "g", lease, nil, nil)
	_, refused["ack of too many"] = c.Stream.GroupAck("s", "g", lease, make([]StreamGroupItem, MaxGroupBatch+1), nil)
	_, refused["nack error over"] = c.Stream.GroupNack("s", "g", lease, one, &StreamGroupNackOptions{Error: strings.Repeat("e", 256)})
	_, refused["nack delay over"] = c.Stream.GroupNack("s", "g", lease, one, &StreamGroupNackOptions{DelayMS: MaxQueueDelayMS + 1})
	_, refused["create lease under"] = c.Stream.GroupCreate("s", "g", &StreamGroupCreateOptions{AckTimeoutMS: Uint32Ptr(MinGroupAckTimeoutMS - 1)})
	_, refused["create lease over"] = c.Stream.GroupCreate("s", "g", &StreamGroupCreateOptions{AckTimeoutMS: Uint32Ptr(MaxGroupAckTimeoutMS + 1)})
	zero := uint8(0)
	_, refused["create max deliver 0"] = c.Stream.GroupCreate("s", "g", &StreamGroupCreateOptions{MaxDeliver: &zero})
	for name, err := range refused {
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestGroupCreateSendsItsSettingsAndInfoReadsItsCounts(t *testing.T) {
	var value, options []byte
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		_, value, options = requestParts(t, p)
		if op == OpStreamGroupInfo {
			return StatusOK, cat(u64le(3), []byte{1, 0, 0, 0}, u64le(99), u64le(1), u64le(2), u64le(4), u64le(0))
		}
		return StatusOK, []byte{1}
	})
	md := uint8(2)
	made, err := c.Stream.GroupCreate("s", "g", &StreamGroupCreateOptions{AckTimeoutMS: Uint32Ptr(5000), MaxDeliver: &md})
	if err != nil || !made {
		t.Fatalf("create = %v, %v", made, err)
	}
	if want := cat([]byte{byte(OptAckTimeoutMS), 4}, []byte{0x88, 0x13, 0, 0}, []byte{byte(OptMaxDeliver), 1, 2}); !bytes.Equal(value, []byte{1, 0, 'g'}) || !bytes.Equal(options, want) {
		t.Errorf("create value % x options % x, want 01 00 67 and % x", value, options, want)
	}
	info, err := c.Stream.GroupInfo("s", "g", nil)
	if err != nil {
		t.Fatal(err)
	}
	if *info != (StreamGroupInfo{Pending: 3, Holders: 1, CreatedAtNS: 99, Ready: 1, Leased: 2, Dead: 4}) {
		t.Errorf("info %+v", info)
	}
}

// keys_not_colocated is a bad request that didn't run: returned at once,
// never retried, with its reason.
func TestKeysNotColocatedIsReturnedNotRetried(t *testing.T) {
	var sends atomic.Int32
	msg := "bad request: this key routes elsewhere than the key the transaction was begun with; give every key in a transaction the same {tag}, e.g. {user:1}:balance"
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		sends.Add(1)
		return StatusBadRequest, refusalBody(ReasonKeysNotColocated, RanNo, msg)
	})
	txn := &KVTransaction{client: c, namespace: "ns", id: 1}
	_, err := txn.Put("plain", []byte("v"), nil)
	var se *ServerError
	if !errors.As(err, &se) || se.Reason != ReasonKeysNotColocated || se.Ran != RanNo || !IsBadRequest(err) || se.Message != msg {
		t.Fatalf("put = %v, want keys_not_colocated", err)
	}
	if n := sends.Load(); n != 1 {
		t.Errorf("sent %d times, want once", n)
	}
	if ReasonKeysNotColocated.String() != "keys_not_colocated" {
		t.Errorf("reason 18 is %q", ReasonKeysNotColocated)
	}
}

// A limit refusal is terminal for the client: returned at once as ErrLimit,
// carrying its reason and the server's message, never retried.
func TestALimitIsReturnedAsErrLimitAndNeverRetried(t *testing.T) {
	var sends atomic.Int32
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		sends.Add(1)
		return StatusLimit, refusalBody(ReasonTooManyActions, RanNo, "limit: this node holds 10000 actions")
	})
	err := c.Action.Register("a", ActionTypeUser, nil)
	var se *ServerError
	if !errors.Is(err, ErrLimit) || !IsLimit(err) || !errors.As(err, &se) || se.Reason != ReasonTooManyActions || se.Message != "limit: this node holds 10000 actions" {
		t.Fatalf("register = %v, want ErrLimit with too_many_actions", err)
	}
	if n := sends.Load(); n != 1 {
		t.Errorf("sent %d times, want once", n)
	}
	if IsOverloaded(err) || IsUnavailable(err) || isRefusedUnrun(err) {
		t.Error("a limit reads as retryable")
	}
	for r, name := range map[Reason]string{
		ReasonMemoryBudget:               "memory_budget",
		ReasonTooManyActions:             "too_many_actions",
		ReasonTooManyWorkflowDefinitions: "too_many_workflow_definitions",
		ReasonTooManyWorkflowRuns:        "too_many_workflow_runs",
	} {
		if got := newServerError(StatusLimit, refusalBody(r, RanNo, "m")); !IsLimit(got) || !strings.Contains(got.Error(), name) {
			t.Errorf("reason %d decoded as %v, want %s", r, got, name)
		}
	}
}

// A worker loop refused at a limit backs off and keeps going, warning once
// per reason rather than on every wait.
func TestAWorkerLoopWarnsOfALimitOncePerReason(t *testing.T) {
	for _, stream := range []bool{false, true} {
		poll := OpActionAwait
		if stream {
			poll = OpStreamGroupRead
		}
		s, addr := newFrameServer(t, func(op OpCode, n int) reply {
			switch {
			case op == poll && n < 2:
				return reply{status: StatusLimit, body: refusalBody(ReasonMemoryBudget, RanNo, "limit: memory budget spent on node 1")}
			case op == poll && n < 4:
				return reply{status: StatusLimit, body: refusalBody(ReasonTooManyWorkflowRuns, RanNo, "limit: runs")}
			case op == OpWorkerHeartbeat:
				return activeBeat()
			case op == poll:
				time.Sleep(10 * time.Millisecond)
			}
			return reply{}
		})
		var logger *captureLogger
		var stop func()
		var done <-chan error
		if stream {
			sw, _, _ := streamDrainWorker(t, addr, 0)
			logger, stop = sw.logger.(*captureLogger), sw.Stop
			done = startWorker(sw.Start)
		} else {
			w := drainTestWorker(t, addr, succeed)
			logger, stop = w.logger.(*captureLogger), w.Stop
			done = startWorker(w.Start)
		}
		waitFor(t, "polls past the limit", func() bool { return len(s.got(poll)) > 5 })
		select {
		case err := <-done:
			t.Fatalf("stream=%v: Start returned %v at a limit", stream, err)
		default:
		}
		stop()
		waitStart(t, done)
		warned := map[string]int{}
		for _, line := range logger.all() {
			for _, r := range []string{"memory_budget", "too_many_workflow_runs"} {
				if strings.Contains(line, "("+r+")") {
					warned[r]++
				}
			}
		}
		if warned["memory_budget"] != 1 || warned["too_many_workflow_runs"] != 1 {
			t.Errorf("stream=%v: warnings %v, want one per reason; log %q", stream, warned, logger.all())
		}
	}
}

// AutoTouch touches a running record on the schedule an action's is: every
// sixth of its group's lease, which GroupList gives, naming the record's
// delivery; it stops before the ack.
func TestStreamWorkerAutoTouchesOnItsGroupsLease(t *testing.T) {
	list := binary.LittleEndian.AppendUint16(nil, 1)
	list = binary.LittleEndian.AppendUint16(list, 7)
	list = append(list, "default"...)
	list = cat(list, []byte{0, 0, 0, 0}, u64le(0), u64le(0), []byte{0x58, 0x02, 0, 0}, []byte{5}, u64le(0), u64le(0), []byte{0}) // 600 ms lease
	s, addr := newFrameServer(t, func(op OpCode, n int) reply {
		if op == OpStreamGroupList {
			return reply{body: list}
		}
		return reply{}
	})
	sw, err := NewClient(addr, WithTimeout(time.Second)).NewStreamWorker(StreamWorkerOptions{Stream: "s", AutoTouch: true, Logger: &captureLogger{}},
		func(*StreamContext) error { time.Sleep(350 * time.Millisecond); return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sw.Close() })
	sw.ctx, sw.cancel = context.WithCancel(context.Background())
	sw.processRecord("s", testLease, testGroupRecord(), time.Now())
	touches := s.got(OpStreamGroupTouch)
	if n := len(touches); n < 2 || n > 4 {
		t.Fatalf("%d touches in 350 ms at a 100 ms interval, want about 3", n)
	}
	_, value, _ := requestParts(t, touches[0][HeaderSize:])
	want := cat([]byte{7, 0}, []byte("default"), []byte{1, 0, 'c'}, u64le(2), u64le(7), []byte{1, 0, 0, 0}, u64le(7), u64le(1), []byte{1, 0, 0, 0})
	if !bytes.Equal(value, want) {
		t.Errorf("touch value % x, want % x", value, want)
	}
	order := s.arrivals()
	if order[len(order)-1] != OpStreamGroupAck {
		t.Errorf("the ack wasn't last: %v", order)
	}
}
