package flo

import (
	"bytes"
	"errors"
	"testing"
)

// An alter sends retention_age_clear (0x2D) and retention_count_clear (0x2E)
// as void flags, after any bound it sets; a create never sends them.
func TestAlterSendsRetentionClears(t *testing.T) {
	cases := []struct {
		name string
		opts StreamAlterOptions
		want []byte
	}{
		{"clear age", StreamAlterOptions{ClearRetentionAge: true},
			[]byte{0x2D, 0}},
		{"clear count", StreamAlterOptions{ClearRetentionCount: true},
			[]byte{0x2E, 0}},
		{"set age, clear count", StreamAlterOptions{RetentionAgeS: 60, ClearRetentionCount: true},
			NewOptionsBuilder().AddU64(OptRetentionAge, 60).AddFlag(OptRetentionCountClear).Build()},
		{"set and clear one bound is sent as asked", StreamAlterOptions{RetentionCount: 5, ClearRetentionCount: true},
			NewOptionsBuilder().AddU64(OptRetentionCount, 5).AddFlag(OptRetentionCountClear).Build()},
		{"neither", StreamAlterOptions{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, opts := captureRequest(t, func(c *Client) {
				_ = c.Stream.Alter("s", &tc.opts)
			})
			if !bytes.Equal(opts, tc.want) {
				t.Fatalf("options = % x, want % x", opts, tc.want)
			}
		})
	}
	_, opts := captureRequest(t, func(c *Client) {
		_ = c.Stream.Create("s", &StreamCreateOptions{RetentionAgeS: 60, RetentionCount: 5})
	})
	if want := NewOptionsBuilder().AddU64(OptRetentionAge, 60).AddU64(OptRetentionCount, 5).Build(); !bytes.Equal(opts, want) {
		t.Fatalf("create options = % x, want % x", opts, want)
	}
}

// Reason 38 decodes by name, not as an unknown number.
func TestSkippedByRepairDecodes(t *testing.T) {
	err := newServerError(StatusConflict, refusalBody(38, RanNo, "skipped by repair at entry 9"))
	var se *ServerError
	if !errors.As(err, &se) || se.Reason != ReasonSkippedByRepair || !se.Reason.Known() || se.Reason.String() != "skipped_by_repair" {
		t.Fatalf("got %#v, want reason skipped_by_repair", err)
	}
	if isRefusedUnrun(err) {
		t.Fatal("a conflict is not retried")
	}
}
