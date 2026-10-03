package gap

import (
	"net/http"

	"github.com/loopopen/gap/internal/dashboard"
)

func NewDashboardHandler(opts OptionsGetter) http.Handler {
	gapOpts := opts.Options()
	dashOpts := gapOpts.DashboardOptions
	if dashOpts == nil {
		panic("dashboard options is nil, may forget to call gap.UseDashboard")
	}
	return dashboard.NewHandler(gapOpts, dashOpts)
}
