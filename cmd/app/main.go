package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/brunofilhorj/watermill-example/internal/domain"
	"github.com/brunofilhorj/watermill-example/internal/infra/bus"
	"github.com/brunofilhorj/watermill-example/internal/messaging"
)

type OrderCreated struct {
	OrderID string
	UserID  string
	Amount  float64
}

func publishTestMessages(bus domain.Bus, count int) {
	for i := 0; i < count; i++ {
		order := OrderCreated{
			OrderID: fmt.Sprintf("ord-%d", i),
			UserID:  "user-123",
			Amount:  float64(100 + i*150),
		}

		payload, _ := json.Marshal(order)
		msg := message.NewMessage(watermill.NewUUID(), payload)

		bus.Publish("order-created", msg)
		time.Sleep(200 * time.Millisecond)
	}
}

func main() {
	count := flag.Int("count", 4, "quantidade de mensagens")
	flag.Parse()

	bus := bus.NewGoChannelBus()

	router, err := messaging.NewRouter(bus)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Go(func() {
		if err := router.Run(ctx); err != nil {
			log.Println("router finalizado:", err)
		}
	})

	// CORRETO: readiness oficial do Watermill
	<-router.Running()

	fmt.Println("🚀 Sistema pronto!")

	publishTestMessages(bus, *count)

	time.Sleep(3 * time.Second)

	cancel()
	wg.Wait()

	fmt.Println("🎉 Finalizado")
}
