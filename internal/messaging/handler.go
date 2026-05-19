package messaging

import (
	"encoding/json"
	"log"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/brunofilhorj/watermill-kafka-example/internal/domain"
	"github.com/brunofilhorj/watermill-kafka-example/internal/usecase"
)

func OrderHandler(msg *message.Message) error {
	log.Println("🔥 CHEGOU NO HANDLER")
	correlationID := msg.Metadata.Get("correlation_id")
	log.Printf("handler started | correlation_id=%s", correlationID)

	var order domain.Order

	if err := json.Unmarshal(msg.Payload, &order); err != nil {
		return err
	}

	return usecase.ProcessOrder(order)
}
