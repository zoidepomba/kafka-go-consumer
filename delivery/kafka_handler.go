package delivery

import (
    "context"
    "kafka-go-consumer/repository"
    "kafka-go-consumer/usecase"
    "log"
    "os"
    "os/signal"
    "syscall"
)

func StartKafkaConsumer() {
    brokers := []string{"localhost:9092"}
    topic := "example-topic"
    groupID := "example-group"

    handler := usecase.NewMessageConsumer()
    consumer := repository.NewKafkaConsumer(brokers, topic, groupID, handler)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go func() {
        consumer.Start(ctx)
    }()

    log.Println("Kafka consumer started...")

    // Graceful shutdown
    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    <-sigs
    log.Println("Shutting down Kafka consumer...")
}