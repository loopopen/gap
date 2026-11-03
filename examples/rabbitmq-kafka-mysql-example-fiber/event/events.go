package event

type Hello struct {
	Name string
	By   string
}

func (e Hello) Topic() string {
	return "topic.greet.hello"
}
