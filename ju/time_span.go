package ju

import (
	"fmt"
	"time"
)

type TimeSpan struct {
	ms int64
}

func (ts *TimeSpan) Start() {
	ts.ms = time.Now().UnixMilli()
}
func (ts *TimeSpan) SpanMs() int64 {
	return time.Now().UnixMilli() - ts.ms
}
func (ts *TimeSpan) String() string {
	ms := time.Now().UnixMilli() - ts.ms
	minutes := ms / 60000
	seconds := (ms % 60000) / 1000
	milliseconds := ms % 1000

	return fmt.Sprintf("%02d:%02d.%03d", minutes, seconds, milliseconds)
}
func (ts *TimeSpan) LogGreen(title string) {
	OutputColor(1, ColorGreen, title+": ", ts.String())
}
