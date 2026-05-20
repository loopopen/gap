package dashboard

import (
	"strings"
)

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//shoot: def="/dashboard"
	PathPrefix string

	LocationPath string
}

// NormalPrefix normalizes prefix like /dashboard
func (o *Options) NormalPrefix() string {
	prefix := "/" + strings.Trim(o.PathPrefix, "/")
	return prefix
}
