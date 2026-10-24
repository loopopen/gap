package gapc

import (
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

var (
	HandleTopicWithinGroupRaw = ingap.HandleTopicWithinGroupRaw
	FileName                  = ingap.FileName
	FuncName                  = ingap.FuncName
	Resolve                   = ingap.Resolve
)

func ResolveType[T any](typeName string, resolve func(v T)) shoot.Option[ingap.DependencyOptions, *ingap.DependencyOptions] {
	return ingap.ResolveType(typeName, resolve)
}
