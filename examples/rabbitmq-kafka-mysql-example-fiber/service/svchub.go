package service

import (
	"database/sql"

	"github.com/loopopen/gap"
)

type SvcHub struct {
	db       *sql.DB
	PubR     gap.EventPublisher
	PubK     gap.EventPublisher
	GreetSvc *GreetSvc
}

func NewSvcHub(db *sql.DB, pubR, pubK gap.EventPublisher) *SvcHub {
	return &SvcHub{
		db:   db,
		PubR: pubR,
		PubK: pubK,
	}
}

func (s *SvcHub) Init() {
	s.GreetSvc = NewGreetSvc(s.db, s.PubR, s.PubK)
}
