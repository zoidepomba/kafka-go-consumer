package repository

import (
    "context"
    "kafka-go-consumer/domain"
    "kafka-go-consumer/usecase"
    "log"

    "github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
    reader         *kafka.Reader
    messageHandler usecase.MessageConsumer
}

//construtor que cria uma nova instância de KafkaConsumer
func NewKafkaConsumer(brokers []string, topic, groupID string, handler usecase.MessageConsumer) *KafkaConsumer {
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  brokers,
        Topic:    topic,
        GroupID:  groupID,
        MinBytes: 10e3, // 10KB
        MaxBytes: 10e6, // 10MB
    })

    return &KafkaConsumer{
        reader:         reader,
        messageHandler: handler,
    }
}

func (kc *KafkaConsumer) Start(ctx context.Context) {
    defer kc.reader.Close()

    for {
        msg, err := kc.reader.ReadMessage(ctx)
        if err != nil {
            log.Println("Error reading message:", err)
            continue
        }

        domainMessage := domain.Message{
            Key:   string(msg.Key),
            Value: string(msg.Value),
        }

        if err := kc.messageHandler.ConsumeMessage(domainMessage); err != nil {
            log.Println("Error processing message:", err)
        }
    }
}