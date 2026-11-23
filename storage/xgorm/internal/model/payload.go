package model

import (
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type Payload string

func (Payload) GormDataType() string {
	return "text"
}

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
