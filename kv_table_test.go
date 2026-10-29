package flo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// tableBody splits a table op's payload: [namespace][key][body].
func tableBody(t *testing.T, p []byte) (namespace, key string, body []byte) {
	t.Helper()
	n := int(binary.LittleEndian.Uint16(p))
	namespace = string(p[2 : 2+n])
	off := 2 + n
	k := int(binary.LittleEndian.Uint16(p[off:]))
	key = string(p[off+2 : off+2+k])
	return namespace, key, p[off+2+k:]
}

func u64le(v uint64) []byte { return binary.LittleEndian.AppendUint64(nil, v) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func TestPutSendsTheSetBodyFieldByField(t *testing.T) {
	ttl, cas := uint64(90_500), uint64(7)
	p := capturePayload(t, func(c *Client) {
		c.KV.Put("k", []byte("v"), &PutOptions{TTLMs: &ttl, Condition: ConditionIfAbsent, CASVersion: &cas, RoutingKey: "rk"})
	})
	_, key, body := tableBody(t, p)
	if key != "k" {
		t.Fatalf("key = %q", key)
	}
	want := cat([]byte{1, 0, 0, 0, 'v'}, []byte{1}, u64le(ttl), []byte{1, 1}, []byte{1}, u64le(cas), []byte{1, 2, 0, 'r', 'k'}, []byte{0})
	if !bytes.Equal(body, want) {
		t.Errorf("set body = % x, want % x", body, want)
	}
}

func TestPutWithoutOptionsSendsDefaultsAndAbsences(t *testing.T) {
	zero := uint64(0)
	p := capturePayload(t, func(c *Client) { c.KV.Put("k", nil, &PutOptions{TTLMs: &zero}) })
	_, _, body := tableBody(t, p)
	// An empty value; a TTL of 0 is none; condition always; no cas, routing key or txn.
	if want := []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0}; !bytes.Equal(body, want) {
		t.Errorf("set body = % x, want % x", body, want)
	}
}

func TestGetSendsTheGetBody(t *testing.T) {
	p := capturePayload(t, func(c *Client) {
		c.KV.Get("k", &GetOptions{WaitMs: 5000, WaitFor: WaitForChange, RoutingKey: "rk"})
	})
	_, _, body := tableBody(t, p)
	want := cat([]byte{1}, u64le(5000), []byte{1, 1}, []byte{1, 2, 0, 'r', 'k'}, []byte{0})
	if !bytes.Equal(body, want) {
		t.Errorf("get body = % x, want % x", body, want)
	}
	if got := waitMS(OpKVGet, body, nil); got != 5000 {
		t.Errorf("waitMS = %d, want 5000: the read deadline must cover the wait", got)
	}
}

func TestTransactionOpsCarryTheirTxnAndCommitNoRoutingKey(t *testing.T) {
	txn := &KVTransaction{namespace: "ns", routingKey: "rk", id: 0x0003_0000_0000_0007}
	p := capturePayload(t, func(c *Client) { txn.client = c; txn.Get("k") })
	_, _, body := tableBody(t, p)
	want := cat([]byte{1}, u64le(0), []byte{1, 0}, []byte{1, 2, 0, 'r', 'k'}, []byte{1}, u64le(txn.id))
	if !bytes.Equal(body, want) {
		t.Errorf("txn get body = % x, want % x", body, want)
	}
	p = capturePayload(t, func(c *Client) { txn.client = c; txn.Put("k", []byte("v"), nil) })
	_, _, body = tableBody(t, p)
	if !bytes.HasSuffix(body, cat([]byte{1, 2, 0, 'r', 'k'}, []byte{1}, u64le(txn.id))) {
		t.Errorf("txn put body = % x: want the routing key and the txn last", body)
	}
	rkTxn := cat([]byte{1, 2, 0, 'r', 'k'}, []byte{1}, u64le(txn.id))
	for name, call := range map[string]func(){
		"delete": func() { txn.Delete("k", nil) },
		"incr":   func() { txn.Incr("k", 3) },
		"expire": func() { txn.Expire("k", 10, nil) },
		"exists": func() { txn.Exists("k") },
	} {
		p = capturePayload(t, func(c *Client) { txn.client = c; call() })
		if _, key, body := tableBody(t, p); key != "k" || !bytes.HasSuffix(body, rkTxn) {
			t.Errorf("txn %s key %q body % x: want the routing key and the txn last", name, key, body)
		}
	}
	casRkTxn := cat([]byte{1}, u64le(5), rkTxn)
	for name, call := range map[string]func(){
		"delete": func() { txn.Delete("k", &KVTxnCASOptions{CASVersion: Uint64Ptr(5)}) },
		"expire": func() { txn.Expire("k", 10, &KVTxnCASOptions{CASVersion: Uint64Ptr(5)}) },
	} {
		p = capturePayload(t, func(c *Client) { txn.client = c; call() })
		if _, _, body := tableBody(t, p); !bytes.HasSuffix(body, casRkTxn) {
			t.Errorf("txn %s with cas body % x: want cas 5, the routing key and the txn last", name, body)
		}
	}
	for name, call := range map[string]func(){
		"commit":   func() { txn.Commit() },
		"rollback": func() { txn.Rollback() },
	} {
		txn.done = false
		p = capturePayload(t, func(c *Client) { txn.client = c; call() })
		if _, key, body := tableBody(t, p); key != "" || !bytes.Equal(body, u64le(txn.id)) {
			t.Errorf("%s key %q body % x: want no key and only the txn", name, key, body)
		}
	}
}

func TestOutOfBoundFieldsAreRefusedBeforeARoundTrip(t *testing.T) {
	c := NewClient("localhost:1")
	if _, err := c.KV.Get("k", &GetOptions{WaitMs: MaxWaitMS + 1}); !errors.Is(err, ErrBlockTooLong) {
		t.Errorf("WaitMs over: %v", err)
	}
	if _, err := c.KV.Put("k", make([]byte, MaxKVValueSize+1), nil); !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("value over: %v", err)
	}
	if _, err := c.KV.Put("k", nil, &PutOptions{Condition: 3}); err == nil {
		t.Error("unknown condition: no error")
	}
}

func TestAnswersDecodeWhole(t *testing.T) {
	got, err := decodeGetAnswer(cat(u64le(4), []byte{2, 0, 0, 0}, []byte("hi"), []byte{0}))
	if err != nil || got.Version != 4 || string(got.Value) != "hi" || got.ExpiresAtMs != nil {
		t.Errorf("get answer = %+v, %v", got, err)
	}
	for _, bad := range [][]byte{nil, cat(u64le(4), []byte{3, 0, 0, 0}, []byte("hi"), []byte{0}), cat(u64le(4), []byte{1, 0, 0, 0}, []byte("hi"), []byte{0})} {
		if _, err := decodeGetAnswer(bad); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("get answer % x: %v, want ErrIncompleteResponse", bad, err)
		}
	}
	if r, err := decodeSetAnswer(cat([]byte{1}, u64le(9))); err != nil || r.Version != 9 {
		t.Errorf("set answer = %+v, %v", r, err)
	}
	if r, err := decodeSetAnswer([]byte{0}); err != nil || r.Version != 0 {
		t.Errorf("txn set answer = %+v, %v", r, err)
	}
	if _, err := decodeSetAnswer([]byte{2}); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("bad set answer: %v", err)
	}
}

func TestNewReasonsHaveNames(t *testing.T) {
	for r := ReasonNotFound; r <= ReasonStaleReport; r++ {
		if _, ok := reasonNames[r]; !ok {
			t.Errorf("reason %d has no name", r)
		}
	}
	for r, want := range map[Reason]string{ReasonNoSuchTransaction: "no_such_transaction", ReasonWrongType: "wrong_type", ReasonTooManyTransactions: "too_many_transactions", ReasonStaleReport: "stale_report"} {
		if r.String() != want {
			t.Errorf("reason %d = %q, want %q", r, r.String(), want)
		}
	}
}
