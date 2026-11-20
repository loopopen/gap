package dashboard

import "net/http"

type RouteRecord struct {
	Method  string
	Path    string
	Handler http.Handler
}

// Add registers an HTTP route with the dashboard service.
func (s *BoardSvc) Add(method string, path string, handler http.Handler) {
	s.routes = append(s.routes, RouteRecord{
		Method:  method,
		Path:    path,
		Handler: handler,
	})
}

// Get registers an HTTP GET route.
func (s *BoardSvc) Get(path string, handler http.Handler) {
	s.Add(http.MethodGet, path, handler)
}

// Post registers an HTTP POST route.
func (s *BoardSvc) Post(path string, hanler http.Handler) {
	s.Add(http.MethodPost, path, hanler)
}
