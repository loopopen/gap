package entity

import "time"

type MessageStats struct {
	Total         int64      `json:"total"`
	Pending       int64      `json:"pending"`
	Succeeded     int64      `json:"succeeded"`
	Failed        int64      `json:"failed"`
	Retries       int64      `json:"retries"`
	LastActivity  *time.Time `json:"lastActivity"`
	OldestBacklog *time.Time `json:"oldestBacklog"`
}

type DashboardStats struct {
	Published MessageStats `json:"published"`
	Received  MessageStats `json:"received"`
}
