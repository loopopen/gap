package dashboard

import (
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/loopopen/gap/internal/dashboard/dto"
	"github.com/loopopen/gap/internal/entity"
	"github.com/loopopen/gap/internal/enum"
	"github.com/loopopen/shoot"
)

var metas []*dto.Meta

func AddMeta(metaType enum.PluginKind, pluginType enum.Plugin, version string) {
	metas = append(metas, dto.NewMeta(metaType, pluginType, version))
}

func (s *BoardSvc) HandleAPIs() []RouteRecord {
	s.Get("/api/metas", s.QueryMetas())
	s.Get("/api/messages/published", s.QueryPublished())
	s.Get("/api/messages/published/{id}", s.GetPublishedByID())
	s.Get("/api/messages/received", s.QueryReceived())
	return s.routes
}

func (s *BoardSvc) QueryMetas() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(ContentType, ContentTypeJSON)
		slices.SortFunc(metas, func(a, b *dto.Meta) int {
			return cmp.Compare(a.Typ, b.Typ)
		})
		if err := json.NewEncoder(w).Encode(metas); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

func (s *BoardSvc) QueryPublished() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var page, perPage int
		filter, err := messageFilterFromRequest(r, false)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		pageStr := r.FormValue("page")
		perPageStr := r.FormValue("per_page")
		page, _ = strconv.Atoi(pageStr)
		perPage, _ = strconv.Atoi(perPageStr)

		es, pg, err := s.storage.QueryPublished(r.Context(), filter, entity.NewPagination(page, perPage))
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

func (s *BoardSvc) GetPublishedByID() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
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
		var page, perPage int
		filter, err := messageFilterFromRequest(r, true)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		pageStr := r.FormValue("page")
		perPageStr := r.FormValue("per_page")
		page, _ = strconv.Atoi(pageStr)
		perPage, _ = strconv.Atoi(perPageStr)

		es, pg, err := s.storage.QueryReceived(r.Context(), filter, entity.NewPagination(page, perPage))
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

func messageFilterFromRequest(r *http.Request, includeGroup bool) (*entity.MessageFilter, error) {
	filter := &entity.MessageFilter{
		Topic:   r.FormValue("topic"),
		Payload: r.FormValue("payload"),
	}
	if includeGroup {
		filter.Group = r.FormValue("group")
	}

	if value := r.FormValue("status"); value != "" {
		status, err := shoot.ParseEnum[enum.Status](value)
		if err != nil {
			return nil, fmt.Errorf("invalid status %q: %w", value, err)
		}
		filter.Status = status
	}

	var err error
	filter.CreatedFrom, err = parseMessageTime(r.FormValue("created_from"))
	if err != nil {
		return nil, fmt.Errorf("invalid created_from: %w", err)
	}
	filter.CreatedTo, err = parseMessageTime(r.FormValue("created_to"))
	if err != nil {
		return nil, fmt.Errorf("invalid created_to: %w", err)
	}
	if filter.CreatedFrom != nil && filter.CreatedTo != nil && filter.CreatedFrom.After(*filter.CreatedTo) {
		return nil, fmt.Errorf("created_from must not be after created_to")
	}
	return filter, nil
}

func parseMessageTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04"} {
		parsed, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return &parsed, nil
		}
	}
	return nil, fmt.Errorf("must be RFC3339 or YYYY-MM-DDTHH:MM")
}
