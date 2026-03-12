package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"

	"kafka-a2/helper"
)


func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "pertanyaan-tutorial",
		GroupID: "consumer-group-1",

		StartOffset: kafka.LastOffset,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message from Kafka: %v", err)
			continue
		}

		var event helper.UserEvent
		err = json.Unmarshal(m.Value, &event)
		if err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			continue
		}

		fmt.Printf("Consumed event: UserID=%s, EventType=%s, Timestamp=%d\n", event.UserID, event.EventType, event.Timestamp)
	}
}