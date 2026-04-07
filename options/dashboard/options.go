package dashboard

import (
	"net/http"
	"path"
	"strings"
)

//go:generate go tool shoot new -opt -short -type=Options

type Options struct {
	//shoot: def="/gap-dashboard"
	PathPrefix string

	Route func(method string, path string, handler http.Handler)
}

func (o *Options) NormalPrefix() string {
	prefix := "/" + strings.Trim(o.PathPrefix, "/")
	return prefix
}

func (o *Options) NormalAPIPrefix() string {
	return path.Join("/api", o.NormalPrefix())
}
