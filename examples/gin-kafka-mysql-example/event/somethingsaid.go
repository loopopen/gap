package event

type SomethingSaid struct {
	Words string
}

func (e SomethingSaid) Topic() string {
	return "topic.sothing.said"
}
