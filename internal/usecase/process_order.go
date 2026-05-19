package usecase

import (
	"log"

	"github.com/brunofilhorj/watermill-kafka-example/internal/domain"
	"github.com/brunofilhorj/watermill-kafka-example/internal/errors"
)

func ProcessOrder(order domain.Order) error {
	switch {
	case order.OrderID == "ord-3":
		panic("simulando panic grave")

	case order.Amount > 1000:
		return errors.ErrSaldoInsuficiente

	case order.Amount > 500:
		return errors.ErrEstoqueIndisponivel

	case order.Amount == 0:
		return errors.ErrValorInvalido
	}

	log.Printf("processando pedido order_id=%s amount=%f", order.OrderID, order.Amount)

	return nil
}
