package kafka

import (
	"os"
	"os/signal"

	"github.com/kumparan/go-lib/logger"

	"github.com/Shopify/sarama"
)

// Producer implements sarama producer
type Producer struct {
	Producer sarama.SyncProducer
}

// Consumer implements sarama consumer
type Consumer struct {
	Consumer sarama.Consumer
}

// Message define struct of message it will be send into kafka
type Message struct {
	Topic   string `json:"topic"`
	Content string `json:"content"`
}

// Publish is function to publish message into kafka
func (p *Producer) Publish(msg Message) error {
	logger.Infof("Message receive: %v", msg)
	_, _, err := p.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.StringEncoder(msg.Content),
	})
	if err != nil {
		logger.Infof("Error receive: %v", err)
	}
	return err
}

// Consume is function to consume message from specific topic kafka
func (c *Consumer) Consume(topic string, callback func([]byte)) (offset int64, err error) {

	logger.Infof("Start consuming topic: %v", topic)
	partitionConsumer, err := c.Consumer.ConsumePartition(topic, 0, -1)
	if err != nil {
		logger.Infof("Error receive: %v", err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			logger.Infof("Error receive: %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			callback(msg.Value)
			offset = msg.Offset
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}

	return offset, err
}
