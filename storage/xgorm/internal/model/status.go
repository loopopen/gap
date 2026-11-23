package model

import (
	"database/sql/driver"

	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/gap/storage/internal/xsql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const mysqlStatusType = "ENUM('Pending','Succeeded','Failed')"

type Status enum.Status

func (s Status) String() string {
	return enum.Status(s).String()
}

func (s Status) Value() (driver.Value, error) {
	return enum.Status(s).Value()
}

func (s *Status) Scan(value any) error {
	var scanned enum.Status
	if err := scanned.Scan(value); err != nil {
		return err
	}
	*s = Status(scanned)
	return nil
}

func (Status) GormDataType() string {
	return "string"
}

func (Status) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	if db != nil && db.Dialector != nil && db.Dialector.Name() == xsql.MySQL {
		return mysqlStatusType
	}
	return ""
}
