package service

import "github.com/loopopen/gap"

type SvcHub struct {
	Pub gap.EventPublisher
	*SaySvc
}

func NewSvcHub(pub gap.EventPublisher) *SvcHub {
	return &SvcHub{
		Pub: pub,
	}
}

func (s *SvcHub) Init() {
	s.SaySvc = NewSaySvc(s.Pub)
}
