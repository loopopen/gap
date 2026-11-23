package dashboard

import (
	"strings"
)

//go:generate go tool shoot new -opt -short -type=Options

// Options configures the operational dashboard. It is part of the v1
// application API through gap.UseDashboard.
type Options struct {
	//@def="/dashboard"
	PathPrefix string

	LocationPath string
}

// NormalPrefix normalizes prefix like /dashboard
func (o *Options) NormalPrefix() string {
	prefix := "/" + strings.Trim(o.PathPrefix, "/")
	return prefix
}
