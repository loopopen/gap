package gapc

import (
	ingap "github.com/loopopen/gap/internal/gap"
	"github.com/loopopen/shoot"
)

var (
	// HandleTopicWithinGroupRaw registers a raw handler for a topic and consumer group.
	HandleTopicWithinGroupRaw = ingap.HandleTopicWithinGroupRaw
	// FileName records the source filename used by generated registration code.
	FileName = ingap.FileName
	// FuncName records the handler function name used for dependency resolution.
	FuncName = ingap.FuncName
	// Resolve registers a generated handler dependency resolver.
	Resolve = ingap.Resolve
)

// ResolveType returns the concrete type represented by an option.
func ResolveType[T any](typeName string, resolve func(v T)) shoot.Option[ingap.DependencyOptions, *ingap.DependencyOptions] {
	return ingap.ResolveType(typeName, resolve)
}
