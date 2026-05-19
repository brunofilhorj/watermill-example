package messaging

import (
	"encoding/json"
	"log"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/brunofilhorj/watermill-example/internal/domain"
	"github.com/brunofilhorj/watermill-example/internal/usecase"
)

func OrderHandler(msg *message.Message) error {
	log.Println("🔥 CHEGOU NO HANDLER")
	correlationID := msg.Metadata.Get("correlation_id")
	log.Printf("handler started | msg_id=%s correlation_id=%s", msg.UUID, correlationID)

	var order domain.Order

	if err := json.Unmarshal(msg.Payload, &order); err != nil {
		return err
	}

	return usecase.ProcessOrder(order)
}
