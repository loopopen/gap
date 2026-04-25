package service

import "github.com/lopolopen/gap"

type SvcContext struct {
	Pub gap.EventPublisher
	*GreetSvc
}

func NewSvcContext(pub gap.EventPublisher) *SvcContext {
	return &SvcContext{
		Pub: pub,
	}
}

func (s *SvcContext) Init() {
	s.GreetSvc = NewGreetSvc(s.Pub)
}
