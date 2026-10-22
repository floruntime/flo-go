package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// actionStatusAnswer is action.status's answer as the server encodes it.
func actionStatusAnswer(status RunStatus, started, completed *int64, attempt uint32, output []byte, errMsg, worker *string) []byte {
	b := []byte{byte(status)}
	b = binary.LittleEndian.AppendUint64(b, 1000)
	for _, t := range []*int64{started, completed} {
		if t == nil {
			b = append(b, 0)
		} else {
			b = binary.LittleEndian.AppendUint64(append(b, 1), uint64(*t))
		}
	}
	b = binary.LittleEndian.AppendUint32(b, attempt)
	if output == nil {
		b = append(b, 0)
	} else {
		b = append(binary.LittleEndian.AppendUint16(append(b, 1), uint16(len(output))), output...)
	}
	if errMsg == nil {
		b = append(b, 0)
	} else {
		b = append(binary.LittleEndian.AppendUint16(append(b, 1), uint16(len(*errMsg))), *errMsg...)
	}
	if worker == nil {
		b = append(b, 0)
	} else {
		b = append(binary.LittleEndian.AppendUint16(append(b, 1), uint16(len(*worker))), *worker...)
	}
	return b
}

func TestActionStatusDecodesACompletedRunsOutput(t *testing.T) {
	started, completed := int64(1100), int64(1200)
	worker := "host-1-ab12"
	st, err := decodeActionStatusAnswer(actionStatusAnswer(RunStatusCompleted, &started, &completed, 1, []byte(`{"ok":true}`), nil, &worker))
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != RunStatusCompleted || st.CreatedAt != 1000 || *st.StartedAt != 1100 || *st.CompletedAt != 1200 ||
		st.Attempt != 1 || !bytes.Equal(st.Output, []byte(`{"ok":true}`)) || st.ErrorMessage != "" || st.WorkerID != worker {
		t.Fatalf("decoded %+v", st)
	}
}

func TestActionStatusDecodesAPendingRunAndAFailedOne(t *testing.T) {
	st, err := decodeActionStatusAnswer(actionStatusAnswer(RunStatusPending, nil, nil, 0, nil, nil, nil))
	if err != nil || st.Status != RunStatusPending || st.StartedAt != nil || st.Output != nil || st.WorkerID != "" {
		t.Fatalf("pending: %+v, %v", st, err)
	}
	msg := "boom"
	st, err = decodeActionStatusAnswer(actionStatusAnswer(RunStatusTimedOut, nil, nil, 3, nil, &msg, nil))
	if err != nil || st.Status != RunStatusTimedOut || st.Attempt != 3 || st.ErrorMessage != "boom" {
		t.Fatalf("timed out: %+v, %v", st, err)
	}
}

func TestActionStatusRefusesACutAnswerAndAnUnknownStatus(t *testing.T) {
	worker := "w"
	whole := actionStatusAnswer(RunStatusCompleted, nil, nil, 1, []byte("x"), nil, &worker)
	for n := range whole {
		if _, err := decodeActionStatusAnswer(whole[:n]); !errors.Is(err, ErrIncompleteResponse) {
			t.Fatalf("cut at %d: %v", n, err)
		}
	}
	if _, err := decodeActionStatusAnswer(actionStatusAnswer(RunStatus(9), nil, nil, 0, nil, nil, nil)); !errors.Is(err, ErrIncompleteResponse) {
		t.Fatalf("status 9: %v", err)
	}
	if !isTableOp(OpActionStatus) {
		t.Fatal("action.status is a table op")
	}
}
