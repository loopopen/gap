package service

import "github.com/lopolopen/gap"

type SvcContext struct {
	pub    gap.EventPublisher
	SaySvc *SaySvc
}

func NewSvcContext(pub gap.EventPublisher) *SvcContext {
	return &SvcContext{
		pub: pub,
	}
}

func (s *SvcContext) Init() {
	s.SaySvc = NewSaySvc(s.pub)
}
