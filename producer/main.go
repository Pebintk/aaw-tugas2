package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"kafka-a2/helper"
)

func main() {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "pertanyaan-tutorial",
		Balancer: &kafka.LeastBytes{},
	}
	defer w.Close()

	users := []string{"alice", "bob", "charlie", "dave", "eve"}

	for _, user := range users {
		event := helper.UserEvent{
			UserID:    user,
			EventType: "login",
			Timestamp: time.Now(),
		}
		eventBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling event: %v", err)
			continue
		}

		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(user),
				Value: eventBytes,
			},
		)
		if err != nil {
			log.Printf("Error writing message to Kafka: %v", err)
		} else {
			log.Printf("Membuat event %s untuk %s", event.EventType, event.UserID)
		}
	}
}