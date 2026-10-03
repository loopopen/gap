package repo

import "github.com/loopopen/gap"

type OrderRepo interface {
	Bind(txer gap.Txer) (OrderRepo, error)

	Create(order any) error
}
