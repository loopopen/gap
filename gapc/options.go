package gapc

import (
	"github.com/lopolopen/gap/internal/gap"
	"github.com/lopolopen/shoot"
)

var (
	GoGenerated               = gap.GoGenerated
	HandleTopicWithinGroupRaw = gap.HandleTopicWithinGroupRaw
	FileName                  = gap.FileName
	FuncName                  = gap.FuncName
	Resolve                   = gap.Resolve
)

func ResolveType[T any](typeName string, resolve func(v T)) shoot.Option[gap.DependencyOptions, *gap.DependencyOptions] {
	return gap.ResolveType(typeName, resolve)
}
