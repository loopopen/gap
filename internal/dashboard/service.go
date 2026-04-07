package dashboard

import (
	"embed"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"text/template"

	"github.com/lopolopen/gap/internal/registry"
	"github.com/lopolopen/gap/options/dashboard"
	"github.com/lopolopen/gap/options/gap"
	"github.com/lopolopen/gap/storage"
)

//go:embed app/dist
var StaticFiles embed.FS

const DistDir = "app/dist"

const (
	ContentType     = "Content-Type"
	ContentTypeJSON = "application/json"
	ContentTypeHTML = "text/html; charset=utf-8"
)

type BoardSvc struct {
	gapOpts *gap.Options
	opts    *dashboard.Options
	routes  []RouteRecord
	storage storage.Storage
}

func NewBoardSvc(gapOpts *gap.Options, opts *dashboard.Options) *BoardSvc {
	stor := registry.MustGetStorage(gapOpts)

	return &BoardSvc{
		gapOpts: gapOpts,
		opts:    opts,
		storage: stor,
	}
}

func (s *BoardSvc) HandleSPA() http.Handler {
	prefix := s.opts.NormalPrefix()

	sub := must(fs.Sub(StaticFiles, DistDir))

	fileServer := http.StripPrefix(prefix, http.FileServer(http.FS(sub)))
	indexTmpl := template.Must(template.ParseFS(sub, "index.html"))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		relPath := strings.TrimPrefix(r.URL.Path, prefix)
		relPath = strings.TrimPrefix(relPath, "/")

		var info fs.FileInfo
		var err error
		var isIndex bool
		if relPath == "" || relPath == "index.html" {
			isIndex = true
		} else {
			info, err = fs.Stat(sub, relPath)
		}
		if isIndex || err != nil || info.IsDir() {
			// fallback to index.html
			w.Header().Set(ContentType, ContentTypeHTML)
			slog.Info(prefix)
			err = indexTmpl.Execute(w, map[string]any{
				"Base":    prefix + "/",
				"APIBase": s.opts.NormalAPIPrefix(),
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		fileServer.ServeHTTP(w, r)
	})
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
