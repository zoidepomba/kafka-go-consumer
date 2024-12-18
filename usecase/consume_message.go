package usecase

import (
	"kafka-go-consumer/domain"
	"log"
)

type MessageConsumer interface {
    ConsumeMessage(msg domain.Message) error
}

type messageConsumer struct{}

func NewMessageConsumer() MessageConsumer {
    return &messageConsumer{}
}


//metodo
func (mc *messageConsumer) ConsumeMessage(msg domain.Message) error {
    // Processa a mensagem (exemplo: log ou salvar no banco)
    log.Print("Processing message:", msg.Key, msg.Value)
    return nil
}