package flo

import (
	"encoding/binary"
	"testing"
)

// [count=1][seq][ts][tier][partition][key_present=0][payload_len=0][header_count=0]
func tierRecord(tier byte) []byte {
	buf := make([]byte, 4+8+8+1+4+1+4+4)
	binary.LittleEndian.PutUint32(buf, 1)
	buf[20] = tier
	return buf
}

func TestStreamRecordTierIsReadAsTheServerSendsIt(t *testing.T) {
	for b, want := range map[byte]StorageTier{0: StorageTierHot, 1: StorageTierWarm} {
		res, err := parseStreamReadResponse(tierRecord(b))
		if err != nil {
			t.Fatalf("tier %d: %v", b, err)
		}
		if got := res.Records[0].Tier; got != want {
			t.Fatalf("tier %d read as %d, want %d", b, got, want)
		}
	}
	if _, err := parseStreamReadResponse(tierRecord(2)); err == nil {
		t.Fatal("tier 2 was accepted; the server sends only 0 and 1")
	}
}

func TestAStreamReadAnswerShortOfItsCountIsRefused(t *testing.T) {
	two := append(tierRecord(0), tierRecord(0)[4:]...)
	binary.LittleEndian.PutUint32(two, 2)
	if res, err := parseStreamReadResponse(two); err != nil || len(res.Records) != 2 {
		t.Fatalf("whole answer: %v, %v", res, err)
	}
	// Cut at the record boundary: one whole record, but a count of two.
	if res, err := parseStreamReadResponse(two[:len(tierRecord(0))]); err == nil {
		t.Fatalf("an answer one record short returned %d records", len(res.Records))
	}
	// One record with a 40-byte payload under a count of two: the bytes could
	// hold two minimal records, so only reading to the count refuses it.
	big := tierRecord(0)
	binary.LittleEndian.PutUint32(big, 2)
	binary.LittleEndian.PutUint32(big[26:], 40)
	big = append(big[:30], append(make([]byte, 40), big[30:]...)...)
	if res, err := parseStreamReadResponse(big); err == nil {
		t.Fatalf("a one-record answer with a count of two returned %d records", len(res.Records))
	}
	huge := tierRecord(0)
	binary.LittleEndian.PutUint32(huge, 0xffffffff)
	if _, err := parseStreamReadResponse(huge); err == nil {
		t.Fatal("a count the data can't hold was accepted")
	}
}
