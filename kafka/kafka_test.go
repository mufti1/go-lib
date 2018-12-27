package kafka_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/kumparan/go-lib/kafka"
)

func TestPublish(t *testing.T) {
	t.Run("send Message ok", func(t *testing.T) {
		mockedProducer := mocks.NewSyncProducer(t, nil)
		mockedProducer.ExpectSendMessageAndSucceed()

		producer := &kafka.Producer{
			Producer: mockedProducer,
		}

		message := kafka.Message{
			Topic:   "topic-test",
			Content: "content-test",
		}

		err := producer.Publish(message)

		if err != nil {
			t.Errorf("publish message should not be error but find: %v", err)
		}
	})
	t.Run("send Message NOK", func(t *testing.T) {
		mockedProducer := mocks.NewSyncProducer(t, nil)
		mockedProducer.ExpectSendMessageAndFail(fmt.Errorf("Intentionally Error"))

		producer := &kafka.Producer{
			Producer: mockedProducer,
		}

		message := kafka.Message{
			Topic:   "topic-test",
			Content: "content-test",
		}

		err := producer.Publish(message)

		if err == nil {
			t.Errorf("Send mesage should be error")
		}
	})
}

func TestConsumeOK(t *testing.T) {
	mockedConsumer := mocks.NewConsumer(t, nil)
	mockedConsumer.SetTopicMetadata(map[string][]int32{
		"test_topic": {0},
	})

	kafka := &kafka.Consumer{
		Consumer: mockedConsumer,
	}

	msg := []byte{200, 200}
	mockedConsumer.ExpectConsumePartition("test_topic", 0, sarama.OffsetNewest).YieldMessage(&sarama.ConsumerMessage{Value: msg})

	signals := make(chan os.Signal, 1)

	go kafka.Consume("test_topic", func([]byte) {
		mockedConsumer.ExpectConsumePartition("test_topic", 0, sarama.OffsetNewest).YieldMessage(&sarama.ConsumerMessage{Value: msg})
	})
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-timeout:
			signals <- os.Interrupt
			return
		}
	}
}
