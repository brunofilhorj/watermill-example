package mainkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
)

// ====================== TIPOS ======================
type OrderCreated struct {
	OrderID string  `json:"order_id"`
	UserID  string  `json:"user_id"`
	Amount  float64 `json:"amount"`
}

// Erros de negócio mapeados (exemplo)
var (
	ErrSaldoInsuficiente   = errors.New("saldo insuficiente")
	ErrEstoqueIndisponivel = errors.New("estoque indisponível")
)

// ====================== HANDLER (separado) ======================
func processOrder(msg *message.Message) error {
	var order OrderCreated
	if err := json.Unmarshal(msg.Payload, &order); err != nil {
		return fmt.Errorf("falha ao deserializar mensagem: %w", err)
	}

	log.Printf("🔄 Processando pedido %s | Usuário: %s | Valor: %.2f",
		order.OrderID, order.UserID, order.Amount)

	// === Simulação de regras de negócio ===
	switch {
	case order.OrderID == "ord-3":
		panic("simulando panic grave") // será recuperado pelo Recoverer

	case order.Amount > 1000:
		return ErrSaldoInsuficiente // NÃO vai para DLQ

	case order.Amount > 500:
		return ErrEstoqueIndisponivel // NÃO vai para DLQ

	case order.Amount == 0:
		return errors.New("valor inválido") // vai para DLQ após retries
	}

	log.Printf("✅ Pedido %s processado com sucesso!", order.OrderID)
	return nil
}

// ====================== FUNÇÃO PARA SALVAR ERRO MAPEADO ======================
func saveMappedError(msg *message.Message, err error) {
	// Aqui você salvaria no banco (ex: PostgreSQL, DynamoDB, etc.)
	log.Printf("📝 [ERRO MAPEADO] Salvando no banco: %v | Pedido: %s",
		err, msg.Metadata.Get("order_id"))
	// Exemplo: inserir em tabela `erros_negocio` com correlation_id, etc.
}

// ====================== MAIN ======================
func main() {
	logger := watermill.NewStdLogger(false, false)

	// Publishers
	publisher := mustNewKafkaPublisher("localhost:9092", logger)
	defer publisher.Close()

	dlqPublisher := mustNewKafkaPublisher("localhost:9092", logger)
	defer dlqPublisher.Close()

	// Subscriber
	subscriber := mustNewKafkaSubscriber("localhost:9092", logger)
	defer subscriber.Close()

	// Router
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	// ====================== MIDDLEWARES ======================
	router.AddMiddleware(
		middleware.Recoverer, // sempre primeiro
		middleware.CorrelationID,

		middleware.Retry{
			MaxRetries:      3,
			InitialInterval: 300 * time.Millisecond,
			MaxInterval:     3 * time.Second,
			Logger:          logger,
		}.Middleware,

		// ====================== DLQ COM FILTRO ======================
		mustPoisonQueueWithFilter(dlqPublisher, "order-created-dlq"),
	)

	// Adiciona o handler extraído
	router.AddNoPublisherHandler(
		"order_processor",
		"order-created",
		subscriber,
		processOrder, // ← função limpa!
	)

	// ====================== Graceful Shutdown ======================
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := router.Run(ctx); err != nil {
			log.Printf("Router finalizado: %v", err)
		}
	}()

	// ====================== Publicação de teste ======================
	publishTestMessages(publisher)

	time.Sleep(12 * time.Second)
	fmt.Println("✅ Exemplo finalizado!")
}

// ====================== HELPERS ======================
func mustNewKafkaPublisher(brokers string, logger watermill.LoggerAdapter) message.Publisher {
	pub, err := kafka.NewPublisher(
		kafka.PublisherConfig{Brokers: []string{brokers}, Marshaler: kafka.DefaultMarshaler{}},
		logger,
	)
	if err != nil {
		panic(err)
	}
	return pub
}

func mustNewKafkaSubscriber(brokers string, logger watermill.LoggerAdapter) message.Subscriber {
	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	sub, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{brokers},
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
			ConsumerGroup:         "order-processing-group",
		},
		logger,
	)
	if err != nil {
		panic(err)
	}
	return sub
}

func mustPoisonQueueWithFilter(pub message.Publisher, topic string) message.HandlerMiddleware {
	filter := func(err error) bool {
		// Erros de negócio mapeados → NÃO vão para DLQ
		if errors.Is(err, ErrSaldoInsuficiente) || errors.Is(err, ErrEstoqueIndisponivel) {
			return false
		}
		return true // outros erros vão para DLQ após retry
	}

	m, err := middleware.PoisonQueueWithFilter(pub, topic, filter)
	if err != nil {
		panic(err)
	}
	return m
}

func publishTestMessages(pub message.Publisher) {
	for i := 0; i < 8; i++ {
		order := OrderCreated{
			OrderID: fmt.Sprintf("ord-%d", i),
			UserID:  "user-123",
			Amount:  float64(100 + i*150),
		}

		payload, _ := json.Marshal(order)
		msg := message.NewMessage(watermill.NewUUID(), payload)
		msg.Metadata.Set("order_id", order.OrderID)

		_ = pub.Publish("order-created", msg)
		time.Sleep(250 * time.Millisecond)
	}
}
