package kafka

import (
	"os"

	stdLog "log"

	"github.com/Shopify/sarama"
)

type (
	Topic interface {
		Name() string
		Timeout() string
		Retry() int
	}
)

func init() {
	// enable: sarama logger --verbose
	sarama.Logger = stdLog.New(os.Stdout, "[sarama] ", stdLog.LstdFlags)
}
