package domain

import "github.com/ThreeDotsLabs/watermill/message"

type Bus interface {
	message.Publisher
	message.Subscriber
}
