package dashboard

import (
	"net/http"
	"path"
	"sync"

	"github.com/lopolopen/gap/internal/gap"
)

var boardSvc *BoardSvc
var initOnce sync.Once

func InitDashboard(gapOpts *gap.Options) {
	opts := gapOpts.DashboardOptions
	if opts == nil {
		return
	}

	initOnce.Do(func() {
		boardSvc = NewBoardSvc(gapOpts, opts)

		if opts.Route == nil {
			panic("mount func is nil; you need to call dashboard.Mount when UseDashboard")
		}

		for _, r := range boardSvc.HandleAPIs() {
			opts.Route(r.Method, path.Join(opts.NormalAPIPrefix(), r.Path), r.Handler)
		}

		//* must be put bottom
		opts.Route(http.MethodGet, path.Join(opts.NormalPrefix(), "*"), boardSvc.HandleSPA())
	})
}
