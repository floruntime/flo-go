package flo

import "encoding/binary"

// testTask is a task of action "a", run "t1", with a lease of leaseMS.
func testTask(leaseMS uint64) *Task {
	return &Task{
		Delivery:  Delivery{RunID: "t1", Action: "a", Deliveries: 1, Term: 2, Index: 7},
		Namespace: "default",
		Attempt:   1,
		LeaseMS:   leaseMS,
	}
}

// awaitAnswer is action.await's answer holding t, as the server codes it.
func awaitAnswer(t *Task) []byte {
	w := bodyWriter{}
	w.u8(1)
	w.bytes16([]byte(t.RunID))
	w.bytes8([]byte(t.Namespace))
	w.u32(t.Deliveries)
	w.u64(t.Term)
	w.u64(t.Index)
	w.bytes16([]byte(t.Action))
	w.u64(uint64(t.CreatedAtMS))
	w.u32(t.Attempt)
	w.u64(t.LeaseMS)
	for _, s := range []string{t.CallerRunID, t.CallerWorkflowName} {
		if s == "" {
			w.u8(0)
		} else {
			w.u8(1)
			w.bytes16([]byte(s))
		}
	}
	w.bytes32(t.Input)
	return w.buf
}

// oneTask is an await answer holding task t1 of action "a".
func oneTask() []byte { return awaitAnswer(testTask(60000)) }

// noTask is an await's answer when no run was ready in time.
var noTask = []byte{0}

// okBody is what the fakes answer an op with when a test gives no body: the
// server's empty answer, which for an await is no task, and for a touch an
// expiry.
// groupReportBody1 is a group report's answer applying its one record.
var groupReportBody1 = []byte{1, 0, 0, 0, 0, 0, 0, 0}

func okBody(op OpCode) []byte {
	switch op {
	case OpActionAwait:
		return noTask
	case OpActionTouch:
		return binary.LittleEndian.AppendUint64(nil, 1)
	case OpStreamGroupRead:
		return make([]byte, 28) // leased nothing
	case OpStreamGroupAck, OpStreamGroupNack:
		return groupReportBody1
	case OpStreamGroupTouch:
		return append(binary.LittleEndian.AppendUint64(nil, 1), groupReportBody1...)
	}
	return nil
}

// deliveryFields is how a report from worker names testTask's delivery:
// action, worker, deliveries, term, index.
func deliveryFields(worker string) []byte {
	b := binary.LittleEndian.AppendUint16(nil, 1)
	b = append(b, 'a')
	b = binary.LittleEndian.AppendUint16(b, uint16(len(worker)))
	b = append(b, worker...)
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint64(b, 2)
	return binary.LittleEndian.AppendUint64(b, 7)
}
