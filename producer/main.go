package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	// "strings"
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

	for i := 0; i < 10; i++ {
		event := helper.UserEvent{
			UserID:    fmt.Sprintf("user_%d", i),
			EventType: "login",
			Timestamp: time.Now().Unix(),
		}
		eventBytes, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling event: %v", err)
			continue
		}

		err = w.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(fmt.Sprintf("user_%d", i)),
				Value: eventBytes,
			},
		)
		if err != nil {
			log.Printf("Error writing message to Kafka: %v", err)
		} else {
			log.Printf("Produced event for user %s", event.UserID)
		}
	}
}