package gap

import (
	"fmt"

	"github.com/loopopen/shoot"
)

//go:generate go tool shoot new -opt -short -file=$GOFILE

type DependencyOptions struct {
	FileName  string
	PkgPath   string
	FuncName  string
	resolvers []func(values []any) string `new:"-"`
}

func ResolveType[T any](typeName string, resolve func(v T)) shoot.Option[DependencyOptions, *DependencyOptions] {
	return func(o *DependencyOptions) {
		o.resolvers = append(o.resolvers, func(values []any) string {
			var dep T
			var resolved bool
			for _, v := range values {
				if d, ok := v.(T); ok {
					dep = d
					resolved = true
					break
				}
			}
			if !resolved {
				return typeName
			}
			resolve(dep)
			return ""
		})
	}
}

func Resolve(opts ...shoot.Option[DependencyOptions, *DependencyOptions]) shoot.Option[Options, *Options] {
	return func(o *Options) {
		options := new(DependencyOptions).With(opts...)
		o.DependencyOptsLst = append(o.DependencyOptsLst, *options)
	}
}

func (d *DependencyOptions) Resolve(values []any) {
	var unresolved string
	for _, r := range d.resolvers {
		tname := r(values)
		if tname != "" {
			unresolved += fmt.Sprintf("\n🧩 %s", tname)
		}
	}
	if len(unresolved) > 0 {
		panic(fmt.Sprintf("unresolved dependencies at %s: %s", d.FuncName, unresolved))
	}
}
