package gapc

import (
	internalgap "github.com/loopopen/gap/v2/internal/gap"
	"github.com/loopopen/shoot"
)

var (
	HandleTopicWithinGroupRaw = internalgap.HandleTopicWithinGroupRaw
	FileName                  = internalgap.FileName
	FuncName                  = internalgap.FuncName
	Resolve                   = internalgap.Resolve
)

func ResolveType[T any](typeName string, resolve func(v T)) shoot.Option[internalgap.DependencyOptions, *internalgap.DependencyOptions] {
	return internalgap.ResolveType(typeName, resolve)
}
