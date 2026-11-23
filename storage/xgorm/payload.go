package xgorm

import (
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Payload is the serialized message body stored by GORM-backed storage.
type Payload string

// GormDataType tells GORM that Payload is a text value.
func (Payload) GormDataType() string {
	return "text"
}

// GormDBDataType prevents dialector string-size defaults from narrowing the
// payload column to VARCHAR.
func (Payload) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	switch db.Dialector.Name() {
	case "mysql":
		return "LONGTEXT"
	case "postgres":
		return "TEXT"
	default:
		return "TEXT"
	}
}
