package event

type Hello struct {
	Name string
}

func (e Hello) Topic() string {
	return "topic.greet.hello"
}
