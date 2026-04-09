package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/lopolopen/gap/internal/dashboard/dto"
	"github.com/lopolopen/gap/internal/entity"
	"github.com/lopolopen/gap/internal/enum"
	"github.com/lopolopen/shoot"
)

var metas []*dto.Meta

func AddMeta(metaType enum.PluginKind, pluginType enum.Plugin, version string) {
	metas = append(metas, dto.NewMeta(metaType, pluginType, version))
}

func (s *BoardSvc) HandleAPIs() []RouteRecord {
	s.Get("metas", s.QueryMetas())
	s.Get("messages/published", s.QueryPublished())
	const pattern = "messages/published/{id}"
	s.Get(pattern, s.GetPublishedByID(pattern))
	s.Get("messages/received", s.QueryReceived())
	return s.routes
}

func (s *BoardSvc) QueryMetas() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ContentType, ContentTypeJSON)

		if err := json.NewEncoder(w).Encode(metas); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (s *BoardSvc) QueryPublished() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var status enum.Status
		var page, perPage int
		var err error
		statusStr := r.FormValue("status")
		if statusStr != "" {
			status, err = shoot.ParseEnum[enum.Status](statusStr)
			if err != nil {
				status = enum.StatusInvalid
			}
		}
		topic := r.FormValue("topic")

		pageStr := r.FormValue("page")
		perPageStr := r.FormValue("per_page")
		page, _ = strconv.Atoi(pageStr)
		perPage, _ = strconv.Atoi(perPageStr)

		es, pg, err := s.storage.QueryPublished(r.Context(), status, topic, entity.NewPagination(page, perPage))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var msgs []*dto.Message
		for _, e := range es {
			msgs = append(msgs, new(dto.Message).FromEntity(e))
		}
		resp := dto.NewPagedResult(msgs, pg)
		w.Header().Set(ContentType, ContentTypeJSON)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (s *BoardSvc) GetPublishedByID(pattern string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idStr := PathValue(pattern, r.URL.Path)
		if idStr == "" {
			http.Error(w, fmt.Sprintf("404 Get %s", r.URL.Path), http.StatusNotFound)
			return
		}

		var id uint64
		id, _ = strconv.ParseUint(idStr, 10, 64)

		e, err := s.storage.GetPublishedByID(r.Context(), uint(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		msg := new(dto.Message).FromEntity(e)
		w.Header().Set(ContentType, ContentTypeJSON)
		if err := json.NewEncoder(w).Encode(msg); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (s *BoardSvc) QueryReceived() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var status enum.Status
		var page, perPage int
		var err error
		statusStr := r.FormValue("status")
		if statusStr != "" {
			status, err = shoot.ParseEnum[enum.Status](statusStr)
			if err != nil {
				status = enum.StatusInvalid
			}
		}
		topic := r.FormValue("topic")
		group := r.FormValue("group")

		pageStr := r.FormValue("page")
		perPageStr := r.FormValue("per_page")
		page, _ = strconv.Atoi(pageStr)
		perPage, _ = strconv.Atoi(perPageStr)

		es, pg, err := s.storage.QueryReceived(r.Context(), status, topic, group, entity.NewPagination(page, perPage))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var msgs []*dto.Message
		for _, e := range es {
			msgs = append(msgs, new(dto.Message).FromEntity(e))
		}
		resp := dto.NewPagedResult(msgs, pg)
		w.Header().Set(ContentType, ContentTypeJSON)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}
