package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
	"github.com/segmentio/kafka-go"

	"kafka-a2/helper"
)


func main() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "tugas-a2",
		GroupID: "login-consumer-1",

		StartOffset: kafka.LastOffset,
	})
	defer r.Close()

	fmt.Println(" [*] Login Service menunggu event dari Kafka. Tekan CTRL+C untuk keluar.")

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

		fmt.Printf("Menerima event untuk UserID=%s, EventType=%s, Timestamp=%s\n", event.UserID, event.EventType, event.Timestamp.String())
		time.Sleep(2 * time.Second)
	}
}