package dashboard

import (
	"net/http"

	"github.com/lopolopen/gap/dashboard"
	"github.com/lopolopen/gap/internal/gap"
)

type Handler struct {
	*http.ServeMux
}

func NewHandler(gapOpts *gap.Options, opts *dashboard.Options) http.Handler {
	h := &Handler{http.NewServeMux()}
	svc := NewBoardSvc(gapOpts, opts)

	h.Handle("GET /api/metas", svc.QueryMetas())
	h.Handle("GET /api/messages/published", svc.QueryPublished())
	h.Handle("GET /api/messages/published/{id}", svc.GetPublishedByID())
	h.Handle("GET /api/messages/received", svc.QueryReceived())
	h.Handle("GET /{any...}", svc.HandleSPA())

	return http.StripPrefix(opts.NormalPrefix(), h)
}
