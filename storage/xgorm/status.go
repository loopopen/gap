package xgorm

import (
	"database/sql/driver"

	"github.com/loopopen/gap/v2/spi/enum"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const mysqlStatusType = "ENUM('Pending','Processing','Succeeded','Failed')"

func (s Status) String() string {
	return enum.Status(s).String()
}

func (s Status) Value() (driver.Value, error) {
	return enum.Status(s).Value()
}

func (s *Status) Scan(value any) error {
	var status enum.Status
	if err := status.Scan(value); err != nil {
		return err
	}
	*s = Status(status)
	return nil
}

// GormDataType tells GORM that Status is stored as a string value.
func (Status) GormDataType() string {
	return "string"
}

// GormDBDataType supplies the native MySQL enum type. Other dialects use
// their normal string type; PostgreSQL is converted to status_enum by init.
func (Status) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	if db != nil && db.Dialector != nil && db.Dialector.Name() == "mysql" {
		return mysqlStatusType
	}
	return ""
}
