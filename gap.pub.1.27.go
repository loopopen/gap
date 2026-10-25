//go:build go1.27

package gap

import "github.com/loopopen/shoot"

func (g *Gap) NewPublisher[T any](opts ...shoot.Option[BrokerOptions, *BrokerOptions]) Publisher[T] {
	return NewPublisher[T](g, opts...)
}
