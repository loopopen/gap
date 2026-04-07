package entity

const (
	MaxPageSize int = 200
	DefPageSize int = 20
)

//go:generate go tool shoot new -json -file=$GOFILE

type Pagination struct {
	//shoot: new
	Page int
	//shoot: new
	PerPage   int
	Total     int
	TotalPage int
}

func (p *Pagination) SetTotal(total int) {
	p.Total = total
	p.TotalPage = (total + p.PerPage - 1) / p.PerPage
}

func (p *Pagination) Normalize() Pagination {
	var page int
	var perPage int
	if p != nil {
		page = p.Page
		perPage = p.PerPage
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
		Page:    page,
		PerPage: perPage,
	}
}
