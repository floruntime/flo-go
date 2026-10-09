package flo

import (
	"bytes"
	"testing"
)

// Trim sends the bounds the server reads: limit (0x05), max_age_seconds
// (0x26), stream_start (0x21) and dry_run (0x28). The retention_* tags it
// used to send are not trim bounds, so every trim was refused.
func TestTrimSendsServerBounds(t *testing.T) {
	cases := []struct {
		name string
		opts StreamTrimOptions
		want []byte
	}{
		{"maxlen dry run", StreamTrimOptions{MaxLen: Uint64Ptr(10), DryRun: true},
			NewOptionsBuilder().AddU64(OptLimit, 10).AddFlag(OptDryRun).Build()},
		{"maxage", StreamTrimOptions{MaxAgeSeconds: Uint64Ptr(60)},
			NewOptionsBuilder().AddU64(OptMaxAgeSeconds, 60).Build()},
		{"before", StreamTrimOptions{Before: &StreamID{TimestampMS: 7, Sequence: 2}},
			NewOptionsBuilder().AddBytes(OptStreamStart, StreamID{TimestampMS: 7, Sequence: 2}.ToBytes()).Build()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, opts := captureRequest(t, func(c *Client) {
				_, _ = c.Stream.Trim("s", &tc.opts)
			})
			if !bytes.Equal(opts, tc.want) {
				t.Fatalf("options = %x, want %x", opts, tc.want)
			}
			if opts[0] != byte(OptLimit) && opts[0] != byte(OptMaxAgeSeconds) && opts[0] != byte(OptStreamStart) {
				t.Fatalf("first option tag %#x is not a trim bound", opts[0])
			}
		})
	}
}
