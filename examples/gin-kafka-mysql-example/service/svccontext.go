package service

import "github.com/lopolopen/gap"

type SvcContext struct {
	Pub    gap.EventPublisher
	SaySvc *SaySvc
}

func NewSvcContext(pub gap.EventPublisher) *SvcContext {
	return &SvcContext{
		Pub: pub,
	}
}

func (s *SvcContext) Init() {
	s.SaySvc = NewSaySvc(s.Pub)
}
