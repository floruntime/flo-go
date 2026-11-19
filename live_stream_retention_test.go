package flo

import (
	"math"
	"strings"
	"testing"
)

// An alter that clears a bound drops it and leaves the other; a bound left
// out or 0 is unchanged.
func TestLiveStreamAlterClearsRetention(t *testing.T) {
	c := liveClient(t)
	if err := c.Stream.Create("kept", &StreamCreateOptions{RetentionAgeS: 3600, RetentionCount: 50}); err != nil {
		t.Fatal(err)
	}
	if err := c.Stream.Alter("kept", &StreamAlterOptions{ClearRetentionAge: true}); err != nil {
		t.Fatalf("clear the age bound: %v", err)
	}
	info, err := c.Stream.Info("kept", nil)
	if err != nil || info.RetentionAgeS != 0 || info.RetentionCount != 50 {
		t.Fatalf("info after clearing the age: %+v, %v; want no age bound, 50 records", info, err)
	}
	if err := c.Stream.Alter("kept", &StreamAlterOptions{RetentionAgeS: 60, ClearRetentionCount: true}); err != nil {
		t.Fatalf("set the age, clear the count: %v", err)
	}
	info, err = c.Stream.Info("kept", nil)
	if err != nil || info.RetentionAgeS != 60 || info.RetentionCount != 0 {
		t.Fatalf("info after clearing the count: %+v, %v; want 60 s, no count bound", info, err)
	}
	if err := c.Stream.Alter("kept", nil); err != nil {
		t.Fatalf("empty alter: %v", err)
	}
	if info, err = c.Stream.Info("kept", nil); err != nil || info.RetentionAgeS != 60 || info.RetentionCount != 0 {
		t.Fatalf("info after an empty alter: %+v, %v; want it unchanged", info, err)
	}
	// With both bounds gone, a create naming none matches.
	if err := c.Stream.Alter("kept", &StreamAlterOptions{ClearRetentionAge: true}); err != nil {
		t.Fatal(err)
	}
	if err := c.Stream.Create("kept", nil); err != nil {
		t.Fatalf("create with no retention after both clears: %v", err)
	}
}

// Setting and clearing one bound in an alter is refused and changes
// nothing; so is a bound of u64 max, on a create and an alter.
func TestLiveStreamRetentionRefusals(t *testing.T) {
	c := liveClient(t)
	if err := c.Stream.Create("s", &StreamCreateOptions{RetentionAgeS: 3600, RetentionCount: 50}); err != nil {
		t.Fatal(err)
	}
	for _, opts := range []StreamAlterOptions{
		{RetentionAgeS: 60, ClearRetentionAge: true},
		{RetentionCount: 5, ClearRetentionCount: true},
	} {
		if err := c.Stream.Alter("s", &opts); !IsBadRequest(err) {
			t.Errorf("alter %+v: %v, want a bad request", opts, err)
		}
	}

	// A cleared count travels as u64 max, so a client can't ask for it.
	if err := c.Stream.Alter("s", &StreamAlterOptions{RetentionCount: math.MaxUint64}); !IsBadRequest(err) || !strings.Contains(err.Error(), "retention bound too large") {
		t.Errorf("alter to a count of u64 max: %v, want refused as too large", err)
	}
	if err := c.Stream.Create("huge", &StreamCreateOptions{RetentionCount: math.MaxUint64}); !IsBadRequest(err) || !strings.Contains(err.Error(), "retention bound too large") {
		t.Errorf("create with a count of u64 max: %v, want refused as too large", err)
	}
	// An age of u64 max is refused by the 100-year cap first.
	if err := c.Stream.Alter("s", &StreamAlterOptions{RetentionAgeS: math.MaxUint64}); !IsBadRequest(err) {
		t.Errorf("alter to an age of u64 max: %v, want a bad request", err)
	}
	if err := c.Stream.Create("huge", &StreamCreateOptions{RetentionAgeS: math.MaxUint64}); !IsBadRequest(err) {
		t.Errorf("create with an age of u64 max: %v, want a bad request", err)
	}

	info, err := c.Stream.Info("s", nil)
	if err != nil || info.RetentionAgeS != 3600 || info.RetentionCount != 50 {
		t.Fatalf("info after the refusals: %+v, %v; want 3600 s, 50 records", info, err)
	}
	if _, err := c.Stream.Info("huge", nil); !IsNotFound(err) {
		t.Fatalf("info of a refused create: %v, want not found", err)
	}
}
