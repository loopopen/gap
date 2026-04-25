package dashboard

import (
	"path"
	"strings"
)

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//shoot: def="/dashboard"
	PathPrefix string

	BaseURL string
}

// NormalPrefix normalizes prefix like /dashboard
func (o *Options) NormalPrefix() string {
	prefix := "/" + strings.Trim(o.PathPrefix, "/")
	return prefix
}

// NormalAPIPrefix normalizes api prefix like /dashboard/api
func (o *Options) NormalAPIPrefix() string {
	return path.Join(o.NormalPrefix(), "api")
}
