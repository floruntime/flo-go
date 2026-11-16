package flo

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// liveTSPoints writes n points of measurement over three series, point i in
// series i%3, two points to each time so series share times. A point's
// value is its index; it returns the points in (time, write order).
func liveTSPoints(t *testing.T, c *Client, measurement string, n int, base int64) []TSPoint {
	t.Helper()
	want := make([]TSPoint, n)
	for i := 0; i < n; i++ {
		at := base + int64(i/2)*1000
		if _, err := c.TS.Write(measurement, float64(i), &TSWriteOptions{Tags: fmt.Sprintf("host=h%d", i%3), TimeNS: at}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		want[i] = TSPoint{TimeNS: at, Value: float64(i)}
	}
	// Written in time order, so this is already (time, write order).
	sort.SliceStable(want, func(a, b int) bool { return want[a].TimeNS < want[b].TimeNS })
	return want
}

func checkTSPoints(t *testing.T, what string, got, want []TSPoint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d points, want %d", what, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: point %d is %+v, want %+v", what, i, got[i], want[i])
		}
	}
}

// 5000 points over three series, some sharing a time, page out each once,
// in order, both ways; a page's cursor alone says whether more follow.
func TestLiveTSReadPagesEveryPointOnceBothWays(t *testing.T) {
	c := liveClient(t)
	base := time.Now().Add(-time.Hour).UnixNano()
	want := liveTSPoints(t, c, "paged", 5000, base)

	for _, order := range []TSOrder{TSOldestFirst, TSNewestFirst} {
		expect := want
		if order == TSNewestFirst {
			expect = make([]TSPoint, len(want))
			for i, p := range want {
				expect[len(want)-1-i] = p
			}
		}
		var got []TSPoint
		var cursor []byte
		pages := 0
		for {
			pages++
			if pages > 20 {
				t.Fatalf("%v: still paging after 20 pages", order)
			}
			page, err := c.TS.Read("paged", &TSReadOptions{Limit: 700, Order: order, Cursor: cursor})
			if err != nil {
				t.Fatalf("%v page %d: %v", order, pages, err)
			}
			if len(page.Points) > 700 {
				t.Fatalf("%v page %d: %d points, limit 700", order, pages, len(page.Points))
			}
			got = append(got, page.Points...)
			if page.Cursor == nil {
				break
			}
			cursor = page.Cursor
		}
		checkTSPoints(t, order.String(), got, expect)
		if pages != 8 {
			t.Errorf("%v: %d pages, want 8", order, pages)
		}

		var walked []TSPoint
		it := c.TS.ReadAll("paged", &TSReadOptions{Limit: 999, Order: order})
		for it.Next() {
			walked = append(walked, it.Point())
		}
		if err := it.Err(); err != nil {
			t.Fatal(err)
		}
		checkTSPoints(t, order.String()+" ReadAll", walked, expect)
	}

	// The newest point, newest first with a limit of 1, is the last written.
	page, err := c.TS.Read("paged", &TSReadOptions{Limit: 1, Order: TSNewestFirst})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Points) != 1 || page.Points[0].Value != 4999 || page.Cursor == nil {
		t.Errorf("newest first, limit 1: %+v", page)
	}

	// A tag filter takes only its series.
	var h1 []TSPoint
	for _, p := range want {
		if int(p.Value)%3 == 1 {
			h1 = append(h1, p)
		}
	}
	var got []TSPoint
	it := c.TS.ReadAll("paged", &TSReadOptions{Tags: "host=h1"})
	for it.Next() {
		got = append(got, it.Point())
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	checkTSPoints(t, "host=h1", got, h1)

	// A raw FloQL answer past 4096 points is refused with the server's text.
	_, err = c.TS.FloQL("paged[2h]", nil)
	if !IsBadRequest(err) || !strings.Contains(err.Error(), "the answer holds more than 4096 points; aggregate them, narrow the query, or page through them with ts read") {
		t.Errorf("FloQL over 4096 points: %v", err)
	}
	series, err := c.TS.FloQL("paged{host=h1}[2h]", nil)
	if err != nil || len(series) != 1 || len(series[0].Points) != len(h1) {
		t.Errorf("FloQL of one series: %d series, %v", len(series), err)
	} else if want := h1[0].TimeNS / 1e6 * 1e6; series[0].Points[0].TimeNS != want {
		// FloQL answers whole ms for now.
		t.Errorf("FloQL time %d ns, want %d", series[0].Points[0].TimeNS, want)
	}
}

// A time that isn't a whole millisecond reads back to the nanosecond.
func TestLiveTSTimeIsNanoseconds(t *testing.T) {
	c := liveClient(t)
	const at = int64(1_700_000_000_123_456_789)
	w, err := c.TS.Write("ns", 1.25, &TSWriteOptions{TimeNS: at})
	if err != nil {
		t.Fatal(err)
	}
	if w.TimeNS != at {
		t.Errorf("write answered %d ns, want %d", w.TimeNS, at)
	}
	from, to := at, at
	page, err := c.TS.Read("ns", &TSReadOptions{FromNS: &from, ToNS: &to})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Points) != 1 || page.Points[0] != (TSPoint{TimeNS: at, Value: 1.25}) || page.Cursor != nil {
		t.Fatalf("read %+v", page)
	}
	if !page.Points[0].Time().Equal(time.Unix(0, at)) {
		t.Errorf("Time() %v", page.Points[0].Time())
	}

	// No time takes the server's, in ns.
	before := time.Now().UnixNano()
	w, err = c.TS.Write("ns", 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if w.TimeNS < before-int64(time.Minute) || w.TimeNS > time.Now().UnixNano()+int64(time.Minute) {
		t.Errorf("server's time %d ns isn't now (%d)", w.TimeNS, before)
	}
}

func TestLiveTSRefusals(t *testing.T) {
	c := liveClient(t)
	if _, err := c.TS.Write("refused", 1, &TSWriteOptions{TimeNS: -1}); !IsBadRequest(err) ||
		!strings.Contains(err.Error(), "ts write: a point's time must not be before 1970") {
		t.Errorf("a time before 1970: %v", err)
	}
	if _, err := c.TS.Write("refused", 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TS.Read("refused", &TSReadOptions{Cursor: []byte{1, 2, 3}}); !IsBadRequest(err) ||
		!strings.Contains(err.Error(), "a cursor is the 16 bytes a page answered") {
		t.Errorf("a 3-byte cursor: %v", err)
	}
	if _, err := c.TS.Read("refused", &TSReadOptions{Tags: "host"}); !IsBadRequest(err) ||
		!strings.Contains(err.Error(), "tag filter part 'host' is not key=value") {
		t.Errorf("a bad tag filter: %v", err)
	}
}

// Close with a run in flight waits for it, so its complete is sent.
func TestLiveActionWorkerCloseSendsTheCompleteOfARunInFlight(t *testing.T) {
	c := liveClient(t)
	w, err := c.NewActionWorker(ActionWorkerOptions{BlockMS: 1000, Logger: &captureLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	running := make(chan struct{})
	w.MustRegisterAction("finish", func(actx *ActionContext) (interface{}, error) {
		close(running)
		<-actx.Context().Done()
		time.Sleep(200 * time.Millisecond)
		return []byte("done"), nil
	}, nil)
	done := make(chan error, 1)
	go func() { done <- w.Start(context.Background()) }()
	run, err := c.Action.Invoke("finish", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-running:
	case <-time.After(15 * time.Second):
		t.Fatal("the worker never ran the action")
	}
	w.Close()
	select {
	case <-done:
	default:
		t.Error("Close returned before Start")
	}
	st, err := c.Action.Status(run.RunID, nil)
	if err != nil || st.Status != RunStatusCompleted || string(st.Output) != "done" {
		t.Fatalf("run after Close: %+v, %v; want completed", st, err)
	}
}

// Close with a record in flight waits for it, so the hand-back of the
// record the stop cut short is sent.
func TestLiveStreamWorkerCloseSendsTheHandBackOfARecordInFlight(t *testing.T) {
	c := liveClient(t)
	if _, err := c.Stream.GroupCreate("closing", "w", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stream.Append("closing", []byte("1"), nil); err != nil {
		t.Fatal(err)
	}
	running := make(chan struct{})
	sw, err := c.NewStreamWorker(StreamWorkerOptions{Stream: "closing", Group: "w", BlockMS: 500, Logger: &captureLogger{}},
		func(sctx *StreamContext) error {
			close(running)
			<-sctx.Ctx().Done()
			time.Sleep(200 * time.Millisecond)
			return sctx.Ctx().Err()
		})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- sw.Start(context.Background()) }()
	select {
	case <-running:
	case <-time.After(10 * time.Second):
		t.Fatal("no handler started")
	}
	sw.Close()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Start = %v", err)
		}
	default:
		t.Error("Close returned before Start")
	}
	info, err := c.Stream.GroupInfo("closing", "w", nil)
	if err != nil || info.Ready != 1 || info.Leased != 0 || info.Dead != 0 {
		t.Fatalf("info after Close: %+v, %v; want the record ready again", info, err)
	}
}
