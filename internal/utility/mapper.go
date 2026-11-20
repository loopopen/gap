package utility

import (
	"database/sql"
	"time"

	"github.com/loopopen/gap/internal/pkgs/timex"
)

type Mapper struct{}

// func (Mapper) DateTimeToTime(dt timex.DateTime) time.Time {
// 	return dt.Time
// }

func (Mapper) TimeToDateTime(t time.Time) timex.DateTime {
	return timex.DateTime{Time: t}
}

func (Mapper) TimeToNullTime(t time.Time) sql.Null[time.Time] {
	return sql.Null[time.Time]{V: t, Valid: !t.IsZero()}
}

func (Mapper) NullTimeToTime(nt sql.Null[time.Time]) time.Time {
	if nt.Valid {
		return nt.V
	}
	return time.Time{}
}
