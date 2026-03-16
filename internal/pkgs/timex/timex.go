package timex

import (
	"fmt"
	"time"
)

type DateTime struct {
	time.Time
}

func (dt DateTime) String() string {
	return dt.Time.Format(time.DateTime)
}

func (dt DateTime) DateString() string {
	return dt.Time.Format(time.DateOnly)
}

// MarshalJSON: convert DateTime to JSON string in "yyyy-MM-dd hh:mm:ss" format
func (dt DateTime) MarshalJSON() ([]byte, error) {
	if dt.IsZero() {
		// encode zero value as empty string
		return []byte(`""`), nil
	}
	s := fmt.Sprintf("\"%s\"", dt.Format(time.DateTime))
	return []byte(s), nil
}

// UnmarshalJSON: parse JSON string in "yyyy-MM-dd hh:mm:ss" format into DateTime
func (dt *DateTime) UnmarshalJSON(b []byte) error {
	// remove surrounding quotes
	s := string(b)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	if s == "" {
		*dt = DateTime{time.Time{}}
		return nil
	}
	t, err := time.ParseInLocation(time.DateTime, s, time.Local)
	if err != nil {
		return err
	}
	dt.Time = t
	return nil
}

func Now() DateTime {
	return DateTime{
		Time: time.Now().Truncate(time.Second),
	}
}

func FromTime(t time.Time) DateTime {
	return DateTime{
		Time: t.Truncate(time.Second),
	}
}
