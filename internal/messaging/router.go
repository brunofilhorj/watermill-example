package messaging

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"

	"github.com/brunofilhorj/watermill-example/internal/domain"
	appErrors "github.com/brunofilhorj/watermill-example/internal/errors"
)

func CustomRecoverer() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (events []*message.Message, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
					log.Printf("🚨 panic recuperado: %v | msg=%s", r, msg.UUID)
				}
			}()
			return h(msg)
		}
	}
}

func PoisonMiddleware(pub message.Publisher) message.HandlerMiddleware {
	filter := func(err error) bool {
		// false = NÃO manda pra DLQ
		if errors.Is(err, appErrors.ErrSaldoInsuficiente) ||
			errors.Is(err, appErrors.ErrEstoqueIndisponivel) {
			log.Printf("🧠 erro de negócio: %v", err)
			return false
		}
		return true
	}

	pm, err := middleware.PoisonQueueWithFilter(pub, "order-created-dlq", filter)
	if err != nil {
		panic(err)
	}

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			events, err := pm(h)(msg)

			if err != nil {
				log.Printf("📤 enviado para DLQ: %s | err=%v", msg.UUID, err)
			}

			return events, err
		}
	}
}

func EnsureCorrelationID() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {

			if msg.Metadata.Get("correlation_id") == "" {
				msg.Metadata.Set("correlation_id", watermill.NewUUID())
			}

			return h(msg)
		}
	}
}

func NewRouter(bus domain.Bus) (*message.Router, error) {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, err
	}

	router.AddMiddleware(
		CustomRecoverer(),
		EnsureCorrelationID(),
		middleware.CorrelationID,
		middleware.Retry{
			MaxRetries:      2,
			InitialInterval: 200 * time.Millisecond,
			Logger:          logger,
		}.Middleware,
		PoisonMiddleware(bus),
	)

	router.AddConsumerHandler(
		"order_processor",
		"order-created",
		bus,
		OrderHandler,
	)

	return router, nil
}
