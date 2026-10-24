package gap

import (
	"net/http"

	"github.com/loopopen/gap/internal/dashboard"
)

func (g *Gap) DashboardHandler() http.Handler {
	gapOpts := g.opts
	dashOpts := gapOpts.DashboardOptions
	if dashOpts == nil {
		panic("dashboard options is nil, may forget to call gap.UseDashboard")
	}
	return dashboard.NewHandler(gapOpts, dashOpts)
}
