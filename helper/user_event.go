package helper

import "time"

type UserEvent struct {
	UserID    string `json:"user_id"`
	EventType string `json:"event_type"`
	Timestamp time.Time  `json:"timestamp"`
}