package bus

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/brunofilhorj/watermill-kafka-example/internal/domain"
)

func NewGoChannelBus() domain.Bus {
	logger := watermill.NewStdLogger(false, false)

	return gochannel.NewGoChannel(
		gochannel.Config{Persistent: true},
		logger,
	)
}
