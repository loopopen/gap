package entity

import (
	"time"

	"github.com/loopopen/gap/internal/enum"
)

type MessageFilter struct {
	ID          *uint
	Status      enum.Status
	Topic       string
	Group       string
	CreatedFrom *time.Time
	CreatedTo   *time.Time
	Payload     string
}
