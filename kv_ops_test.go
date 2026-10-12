package flo

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func hx(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func bptr(v bool) *bool { return &v }

// These vectors come from the server's own schemas.
func TestKVRequestBodiesMatchTheServer(t *testing.T) {
	cas, txn9 := uint64(7), uint64(9)
	enc := func(b []byte, err error) []byte {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"delete", enc(encodeDeleteBody(&cas, "rk", nil)), "010700000000000000010200726b00"},
		{"scan", enc(encodeScanBody(2, []byte("c1"))), "0102000000026331"},
		{"history", enc(encodeHistoryBody(2)), "0102000000"},
		{"mget", enc(encodeMGetBody([]string{"a", "b"})), "0200010061010062"},
		{"incr", enc(encodeIncrBody(-5, "", &txn9)), "01fbffffffffffffff00010900000000000000"},
		{"expire", enc(encodeExpireBody(60000, nil, "", nil)), "60ea000000000000000000"},
		{"exists", enc(encodeExistsBody("", nil)), "0000"},
		{"json_get", enc(encodeJSONPathBody("$.a", "")), "010300242e6100"},
		{"json_set", enc(encodeJSONSetBody("$", []byte("{}"), "rk")), "01010024020000007b7d010200726b"},
		{"json_del", enc(encodeJSONPathBody("$.b", "")), "010300242e6200"},
		{"txn_commit", encodeTxnBody(1), "0100000000000000"},
	}
	for _, c := range cases {
		if want := hx(t, c.want); !bytes.Equal(c.got, want) {
			t.Errorf("%s body = %x, want %x", c.name, c.got, want)
		}
	}
	// An empty path is the root.
	if got := enc(encodeJSONPathBody("", "")); !bytes.Equal(got, hx(t, "0101002400")) {
		t.Errorf("root path body = %x", got)
	}
}

func TestKVAnswersMatchTheServer(t *testing.T) {
	check := func(name string, got, want any, err error) {
		t.Helper()
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %+v, %v; want %+v", name, got, err, want)
		}
	}
	existed, err := decodeDeleteAnswer(hx(t, "0101"))
	check("delete", existed, bptr(true), err)
	existed, err = decodeDeleteAnswer(hx(t, "00"))
	check("delete in txn", existed, (*bool)(nil), err)

	scan, err := decodeScanAnswer(hx(t, "02000100610200626201026332"))
	check("scan", scan, &ScanResult{Keys: []string{"a", "bb"}, Cursor: []byte("c2")}, err)
	scan, err = decodeScanAnswer(hx(t, "000000"))
	check("last scan", scan, &ScanResult{Keys: []string{}}, err)

	hist, err := decodeHistoryAnswer(hx(t, "010300000000000000e803000000000000020000007633"))
	check("history", hist, []VersionEntry{{Version: 3, AtMs: 1000, Value: []byte("v3")}}, err)

	mget, err := decodeMGetAnswer(hx(t, "0200010200000000000000010000007800000000000000000000000000010200"))
	next := uint16(2)
	check("mget", mget, mgetAnswer{entries: []MGetEntry{{Found: true, Version: 2, Value: []byte("x")}, {}}, next: &next}, err)

	incr, err := decodeIncrAnswer(hx(t, "010400000000000000012a00000000000000"))
	check("incr", incr, &IncrResult{Version: 4, Value: 42}, err)
	incr, err = decodeIncrAnswer(hx(t, "0000"))
	check("incr in txn", incr, (*IncrResult)(nil), err)

	check("expire", nil, nil, decodeEmptyAnswer(nil))

	exists, err := decodeExistsAnswer(hx(t, "010500000000000000"))
	check("exists", exists, true, err)

	doc, err := decodeJSONGetAnswer(hx(t, "0300000000000000020000007b7d"))
	check("json_get", doc, &GetResult{Version: 3, Value: []byte("{}")}, err)
	set, err := decodeJSONSetAnswer(hx(t, "0300000000000000"))
	check("json_set", set, &PutResult{Version: 3}, err)
	del, err := decodeJSONDelAnswer(hx(t, "00"))
	check("json_del", del, (*PutResult)(nil), err)

	id, pinned, err := decodeBeginAnswer(hx(t, "01000000000000000200000000000000"))
	check("txn_begin", [2]uint64{id, pinned}, [2]uint64{1, 2}, err)
	commit, err := decodeCommitAnswer(hx(t, "0a000000000000000200"))
	check("txn_commit", commit, &KVCommitResult{CommitIndex: 10, OpCount: 2}, err)
	check("txn_rollback", nil, nil, decodeEmptyAnswer([]byte{}))
}

// Every answer is decoded whole: cut short, or with bytes left over, it is
// refused.
func TestKVAnswersAreDecodedWhole(t *testing.T) {
	decoders := map[string]struct {
		decode func([]byte) error
		good   string
	}{
		"delete":     {func(b []byte) error { _, err := decodeDeleteAnswer(b); return err }, "0101"},
		"scan":       {func(b []byte) error { _, err := decodeScanAnswer(b); return err }, "02000100610200626201026332"},
		"history":    {func(b []byte) error { _, err := decodeHistoryAnswer(b); return err }, "010300000000000000e803000000000000020000007633"},
		"mget":       {func(b []byte) error { _, err := decodeMGetAnswer(b); return err }, "0200010200000000000000010000007800000000000000000000000000010200"},
		"incr":       {func(b []byte) error { _, err := decodeIncrAnswer(b); return err }, "010400000000000000012a00000000000000"},
		"exists":     {func(b []byte) error { _, err := decodeExistsAnswer(b); return err }, "010500000000000000"},
		"json_get":   {func(b []byte) error { _, err := decodeJSONGetAnswer(b); return err }, "0300000000000000020000007b7d"},
		"json_set":   {func(b []byte) error { _, err := decodeJSONSetAnswer(b); return err }, "0300000000000000"},
		"json_del":   {func(b []byte) error { _, err := decodeJSONDelAnswer(b); return err }, "010300000000000000"},
		"txn_begin":  {func(b []byte) error { _, _, err := decodeBeginAnswer(b); return err }, "01000000000000000200000000000000"},
		"txn_commit": {func(b []byte) error { _, err := decodeCommitAnswer(b); return err }, "0a000000000000000200"},
		"empty":      {decodeEmptyAnswer, ""},
	}
	for name, d := range decoders {
		good := hx(t, d.good)
		if err := d.decode(good); err != nil {
			t.Errorf("%s: good answer refused: %v", name, err)
		}
		if err := d.decode(append(append([]byte{}, good...), 0)); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("%s with a trailing byte: %v, want ErrIncompleteResponse", name, err)
		}
		if len(good) > 0 {
			if err := d.decode(good[:len(good)-1]); !errors.Is(err, ErrIncompleteResponse) {
				t.Errorf("%s cut short: %v, want ErrIncompleteResponse", name, err)
			}
		}
	}
	// A presence or bool byte past 1 is malformed.
	for _, bad := range []string{"02", "0102"} {
		if _, err := decodeDeleteAnswer(hx(t, bad)); !errors.Is(err, ErrIncompleteResponse) {
			t.Errorf("delete answer %s: %v", bad, err)
		}
	}
	// Half an increment answer is malformed.
	if _, err := decodeIncrAnswer(hx(t, "0104000000000000000000")); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("incr with a version and no value: %v", err)
	}
}

// tableServer answers each request with the status and data answer returns
// for its op and payload.
func tableServer(t *testing.T, answer func(op OpCode, payload []byte) (StatusCode, []byte)) *Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
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
			status, data := answer(OpCode(binary.LittleEndian.Uint16(header[20:22])), payload)
			resp := make([]byte, HeaderSize+len(data))
			binary.LittleEndian.PutUint32(resp[0:4], Magic)
			binary.LittleEndian.PutUint32(resp[4:8], uint32(len(data)))
			copy(resp[8:16], header[8:16])
			resp[20] = Version
			resp[21] = byte(status)
			binary.LittleEndian.PutUint64(resp[24:32], TableHash)
			copy(resp[HeaderSize:], data)
			binary.LittleEndian.PutUint32(resp[16:20], computeCRC32(resp[:HeaderSize], data))
			if _, err := conn.Write(resp); err != nil {
				return
			}
		}
	}()
	c := NewClient(ln.Addr().String(), WithTimeout(time.Second))
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// mgetEntry codes one kv.mget answer entry.
func mgetEntry(found bool, version uint64, value string) []byte {
	b := []byte{0}
	if found {
		b[0] = 1
	}
	b = binary.LittleEndian.AppendUint64(b, version)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(value)))
	return append(b, value...)
}

func TestMGetFollowsNextUntilEveryKeyIsAnswered(t *testing.T) {
	var mu sync.Mutex
	var asked [][]string
	c := tableServer(t, func(op OpCode, p []byte) (StatusCode, []byte) {
		_, key, body := tableBody(t, p)
		if op != OpKVMGet || key != "" {
			t.Errorf("op 0x%x key %q, want kv.mget with no key", op, key)
		}
		n := int(binary.LittleEndian.Uint16(body))
		keys := make([]string, 0, n)
		for off := 2; len(keys) < n; {
			l := int(binary.LittleEndian.Uint16(body[off:]))
			keys = append(keys, string(body[off+2:off+2+l]))
			off += 2 + l
		}
		mu.Lock()
		asked = append(asked, keys)
		mu.Unlock()
		// Each answer fills up after two keys; a key named "-" is absent.
		var entries []byte
		count := min(2, len(keys))
		for _, k := range keys[:count] {
			if k == "-" {
				entries = append(entries, mgetEntry(false, 0, "")...)
			} else {
				entries = append(entries, mgetEntry(true, 1, "v"+k)...)
			}
		}
		data := cat(binary.LittleEndian.AppendUint16(nil, uint16(count)), entries)
		if count < len(keys) {
			data = append(data, 1, byte(count), 0)
		} else {
			data = append(data, 0)
		}
		return StatusOK, data
	})
	got, err := c.KV.MGet([]string{"a", "-", "c", "d", "e"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []MGetEntry{
		{Key: "a", Found: true, Version: 1, Value: []byte("va")},
		{Key: "-"},
		{Key: "c", Found: true, Version: 1, Value: []byte("vc")},
		{Key: "d", Found: true, Version: 1, Value: []byte("vd")},
		{Key: "e", Found: true, Version: 1, Value: []byte("ve")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("entries = %+v\nwant %+v", got, want)
	}
	if want := [][]string{{"a", "-", "c", "d", "e"}, {"c", "d", "e"}, {"e"}}; !reflect.DeepEqual(asked, want) {
		t.Errorf("asked %v, want %v", asked, want)
	}
}

func TestMGetRefusesAnAnswerThatMakesNoProgress(t *testing.T) {
	c := tableServer(t, func(OpCode, []byte) (StatusCode, []byte) {
		return StatusOK, []byte{0, 0, 1, 0, 0} // no entries, next 0
	})
	if _, err := c.KV.MGet([]string{"a"}, nil); !errors.Is(err, ErrIncompleteResponse) {
		t.Errorf("got %v, want ErrIncompleteResponse", err)
	}
}

func TestDeleteOfAMissingKeyReportsItDidNotExist(t *testing.T) {
	c := tableServer(t, func(OpCode, []byte) (StatusCode, []byte) { return StatusOK, []byte{1, 0} })
	existed, err := c.KV.Delete("gone", nil)
	if err != nil || existed {
		t.Errorf("Delete = %v, %v; want false, nil", existed, err)
	}
}

func TestNotFoundOtherThanTheKeyIsAnError(t *testing.T) {
	c := tableServer(t, func(OpCode, []byte) (StatusCode, []byte) {
		return StatusNotFound, refusalBody(ReasonNoSuchTransaction, RanNo, "no such transaction")
	})
	var se *ServerError
	if _, err := c.KV.JsonGet("k", "", nil); !errors.As(err, &se) || se.Reason != ReasonNoSuchTransaction {
		t.Errorf("JsonGet: %v, want no_such_transaction", err)
	}
	if _, err := c.KV.History("k", nil); !errors.As(err, &se) || se.Reason != ReasonNoSuchTransaction {
		t.Errorf("History: %v, want no_such_transaction", err)
	}
}

func TestKVOpsSendTheirEnvelopeKey(t *testing.T) {
	limit := uint32(2)
	cases := []struct {
		name, key string
		body      []byte
		call      func(c *Client)
	}{
		{"scan names the prefix", "user:", hx(t, "0102000000026331"), func(c *Client) {
			c.KV.Scan("user:", &ScanOptions{Limit: &limit, Cursor: []byte("c1")})
		}},
		{"scan defaults its limit", "", hx(t, "016400000000"), func(c *Client) { c.KV.Scan("", nil) }},
		{"history defaults its limit", "k", hx(t, "010a000000"), func(c *Client) { c.KV.History("k", nil) }},
		{"incr defaults its delta", "k", cat([]byte{1}, u64le(1), []byte{0, 0}), func(c *Client) { c.KV.Incr("k", nil) }},
		{"expire carries its cas", "k", cat(u64le(0), []byte{1}, u64le(4), []byte{0, 0}), func(c *Client) {
			c.KV.Expire("k", 0, &KVExpireOptions{CASVersion: Uint64Ptr(4)})
		}},
		{"begin names the routing key", "rk", nil, func(c *Client) { c.KV.Begin("rk", nil) }},
	}
	for _, tc := range cases {
		_, key, body := tableBody(t, capturePayload(t, tc.call))
		if key != tc.key || !bytes.Equal(body, tc.body) {
			t.Errorf("%s: key %q body %x, want %q %x", tc.name, key, body, tc.key, tc.body)
		}
	}
}

func TestOutOfBoundKVRequestsAreRefusedBeforeARoundTrip(t *testing.T) {
	c := tableServer(t, func(op OpCode, _ []byte) (StatusCode, []byte) {
		t.Errorf("op 0x%x reached the server", op)
		return StatusOK, nil
	})
	zero, overScan, overHistory := uint32(0), uint32(MaxScanLimit+1), uint32(MaxHistoryLimit+1)
	longRK := string(make([]byte, maxRoutingKey+1))
	many := make([]string, MaxMGetKeys+1)
	refused := map[string]error{}
	_, refused["scan limit 0"] = c.KV.Scan("", &ScanOptions{Limit: &zero})
	_, refused["scan limit over"] = c.KV.Scan("", &ScanOptions{Limit: &overScan})
	_, refused["scan cursor over"] = c.KV.Scan("", &ScanOptions{Cursor: make([]byte, maxScanCursor+1)})
	_, refused["history limit 0"] = c.KV.History("k", &HistoryOptions{Limit: &zero})
	_, refused["history limit over"] = c.KV.History("k", &HistoryOptions{Limit: &overHistory})
	_, refused["mget over"] = c.KV.MGet(many, nil)
	refused["expire ttl over"] = c.KV.Expire("k", MaxTTLMs+1, nil)
	_, refused["put ttl over"] = c.KV.Put("k", nil, &PutOptions{TTLMs: Uint64Ptr(MaxTTLMs + 1)})
	_, refused["delete routing key over"] = c.KV.Delete("k", &DeleteOptions{RoutingKey: longRK})
	_, refused["incr routing key over"] = c.KV.Incr("k", &KVIncrOptions{RoutingKey: longRK})
	_, refused["exists routing key over"] = c.KV.Exists("k", &KVExistsOptions{RoutingKey: longRK})
	_, refused["json path over"] = c.KV.JsonGet("k", string(make([]byte, MaxJSONPathSize+1)), nil)
	_, refused["json document over"] = c.KV.JsonSet("k", "$", make([]byte, MaxJSONDocumentSize+1), nil)
	_, refused["begin without routing key"] = c.KV.Begin("", nil)
	_, refused["begin routing key over"] = c.KV.Begin(longRK, nil)
	for name, err := range refused {
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := c.KV.MGet(nil, nil); err != nil {
		t.Errorf("empty mget: %v", err)
	}
	if _, err := c.KV.JsonSet("k", "$", make([]byte, MaxJSONDocumentSize+1), nil); !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("json document over: %v, want ErrValueTooLarge", err)
	}
}

func TestOpsOutsideATransactionAreRefusedInOne(t *testing.T) {
	txn := &KVTransaction{routingKey: "rk", id: 1}
	if _, err := txn.Scan("", nil); !errors.Is(err, ErrTxnUnsupportedOp) {
		t.Errorf("Scan: %v", err)
	}
	if _, err := txn.JsonSet("k", "$", nil, nil); !errors.Is(err, ErrTxnUnsupportedOp) {
		t.Errorf("JsonSet: %v", err)
	}
}

func TestEveryKVOpIsATableOp(t *testing.T) {
	for op := OpKVPut; op <= OpKVExists; op++ {
		want := op <= OpKVHistory || (op >= OpKVIncr && op <= OpKVJsonDel) || (op >= OpKVBeginTxn && op != 0x114 && op != 0x10F)
		if isTableOp(op) != want {
			t.Errorf("isTableOp(0x%x) = %v", op, !want)
		}
	}
}
