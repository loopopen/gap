package gap

import (
	"fmt"
	"net/http"
	"path"

	"github.com/lopolopen/gap/internal/dashboard"
)

var boardSvc *dashboard.BoardSvc

func initDashboard(gapOpts *Options) {
	opts := gapOpts.Dashboard()
	if opts == nil {
		return
	}

	if boardSvc != nil {
		panic("dashboard must be initialized only once")
	}
	boardSvc = dashboard.NewBoardSvc(gapOpts, opts)

	if opts.Route == nil {
		panic("mount func is nil; you need to call dashboard.Mount when UseDashboard")
	}

	prefix := opts.NormalPrefix()

	for _, r := range boardSvc.HandleAPIs() {
		opts.Route(r.Method, addAPIPrefix(prefix, r.Path), r.Handler)
	}

	opts.Route(http.MethodGet, addAPIPrefix(prefix, "*"), http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, fmt.Sprintf("%d %s %s", http.StatusNotFound, http.MethodGet, r.URL.Path), http.StatusNotFound)
		}))

	//* must be put bottom
	opts.Route(http.MethodGet, path.Join(prefix, "*"), boardSvc.HandleSPA())
}

func addAPIPrefix(prefix, subPath string) string {
	return path.Join(prefix, "api", subPath)
}
