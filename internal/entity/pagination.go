package entity

//go:generate go tool shoot new -getset -json -type=Pagination

const (
	MaxPageSize int = 200
	DefPageSize int = 20
)

type Pagination struct {
	//shoot: new
	page int
	//shoot: new
	perPage   int
	Total     int
	TotalPage int
}

func (p *Pagination) SetTotal(total int) {
	p.Total = total
	p.TotalPage = (total + p.perPage - 1) / p.perPage
}

func (p *Pagination) Normalize() Pagination {
	var page int
	var perPage int
	if p != nil {
		page = p.page
		perPage = p.perPage
	}
	if page <= 0 {
		page = 1
	}
	switch {
	case perPage > MaxPageSize:
		perPage = MaxPageSize
	case perPage <= 0:
		perPage = DefPageSize
	}
	return Pagination{
		page:    page,
		perPage: perPage,
	}
}
