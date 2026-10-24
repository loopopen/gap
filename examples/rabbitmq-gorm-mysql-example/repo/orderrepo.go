package repo

import "github.com/loopopen/gap/v2"

type OrderRepo interface {
	Bind(txer gap.Txer) (OrderRepo, error)

	Create(order any) error
}
